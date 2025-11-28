/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StreamScanMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.ChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.paimon.CoreOptions.ChangelogProducer.LOOKUP;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/** {@link StreamTableScan} implementation for streaming planning.
 * 流读数据表，持续不断地将 Paimon 表中的增量数据作为一条条记录发送给下游算子
 *  管理扫描生命周期：它内部组合了 StartingScanner 和 FollowUpScanner。
 *      1.StartingScanner：负责作业启动时的首次扫描。根据不同的启动模式（如 latest, latest-full, from-snapshot），它决定是从最新的快照全量读，还是从某个历史快照增量读。
 *      2.FollowUpScanner：在首次扫描结束后，负责后续的增量扫描。它会周期性地检查是否有新的快照生成，并读取这些新快照带来的变更数据（Deltas 或 Changelogs）。
 *  状态管理：它实现了 checkpoint 和 restore 方法，通过记录和恢复 nextSnapshotId（下一个要读取的快照 ID）来与 Flink 的 Checkpoint 机制对齐，保证 Exactly-Once 语义。
 *  生成执行计划：调用 plan() 方法时，它会根据当前的状态（nextSnapshotId）去扫描对应的快照，并生成需要被读取的数据文件切片（Splits）。
 * */
public class DataTableStreamScan extends AbstractDataTableScan implements StreamDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableStreamScan.class);

    private final CoreOptions options;
    private final StreamScanMode scanMode;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;
    private final NextSnapshotFetcher nextSnapshotProvider; // 下一个快照获取器。封装了发现和获取新快照的逻辑
    private final boolean hasPk;

    private boolean initialized = false;
    private StartingScanner startingScanner; // 起始扫描器。负责处理流作业的“第一次”扫描。根据 startup-mode 的不同，它可能扫描全量数据、从某个历史快照开始扫描，或者直接跳过历史数据。它的工作是一次性的，完成后就由 followUpScanner 接管
    private FollowUpScanner followUpScanner; // 后续增量扫描器。在 startingScanner 完成初始扫描后，followUpScanner 负责持续地、增量地扫描新生成的快照。
    private BoundedChecker boundedChecker; // 有界流检查器。用于判断流作业是否应该结束。例如，可以配置一个水印（watermark），当处理到的快照的水印超过这个值时，就认为流结束了

    private boolean isFullPhaseEnd = false; // 标记初始的全量扫描阶段是否已经结束
    @Nullable private Long currentWatermark; // 当前处理过的快照所携带的水印
    @Nullable private Long nextSnapshotId; // 下一个待处理的快照ID。这是流式处理中最重要的状态。它记录了当前流作业已经处理到哪个快照，下次 plan() 调用时将从这个 ID 开始寻找新的快照。这个值会在 checkpoint 时被持久化，在 restore 时被恢复

    @Nullable private Long scanDelayMillis;

    public DataTableStreamScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean supportStreamingReadOverwrite,
            TableQueryAuth queryAuth,
            boolean hasPk) {
        super(schema, options, snapshotReader, queryAuth);

        this.options = options;
        this.scanMode = options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
        this.nextSnapshotProvider =
                new NextSnapshotFetcher(
                        snapshotManager, changelogManager, options.changelogLifecycleDecoupled());
        this.hasPk = hasPk;

        if (options.bucket() == BucketMode.POSTPONE_BUCKET
                && options.changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @Override
    public DataTableStreamScan withFilter(Predicate predicate) {
        super.withFilter(predicate);
        snapshotReader.withFilter(predicate);
        return this;
    }

    @Override
    public StartingContext startingContext() {
        if (!initialized) {
            initScanner();
        }
        return startingScanner.startingContext();
    }

    /**
     * 核心逻辑，驱动数据读取流程
     * 被调用时，它并不真正地去读取数据文件。它只是检查 Paimon 的元数据（manifest 文件等），找出相比上一次处理新增了哪些数据文件，然后把这些文件的信息打包成一个 Plan 对象返回。
     * Flink 等收到这个 Plan 后，才会根据里面的 Splits 列表，把这些读取任务分发给下游的 Source Task 去具体执行 I/O 操作
     */
    @Override
    public Plan plan() {
        authQuery();

        if (!initialized) {
            initScanner();
        }

        if (nextSnapshotId == null) {
            // 首次 plan, 使用 startingScanner
            return tryFirstPlan();
        } else {
            // 作业已启动或restore，后续 plan, 使用 followUpScanner
            return nextPlan();
        }
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        throw new UnsupportedOperationException(
                "List Partition Entries is not supported in Stream Scan.");
    }

    private void initScanner() {
        if (startingScanner == null) {
            // 调用继承自 AbstractDataTableScan 的方法，并传入 true 表示是为流模式创建扫描器
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            // 根据 changelog-producer 等配置创建后续的增量扫描器。例如，如果配置为 input 或 full-compaction，会创建 ChangelogFollowUpScanner 来读取 changelog 文件；否则创建 DeltaFollowUpScanner 来读取增量数据文件。
            followUpScanner = createFollowUpScanner();
        }
        if (boundedChecker == null) {
            // 根据 scan.bounded.watermark 配置创建有界检查器
            boundedChecker = createBoundedChecker();
        }
        if (scanDelayMillis == null) {
            scanDelayMillis = getScanDelayMillis();
        }
        initialized = true;
    }

    private Plan tryFirstPlan() {
        StartingScanner.Result result;
        if (scanMode == FILE_MONITOR) {
            result = startingScanner.scan(snapshotReader);
        } else if (options.changelogProducer().equals(LOOKUP)) {
            // level0 data will be compacted to produce changelog in the future
            result = startingScanner.scan(snapshotReader.withLevelFilter(level -> level > 0));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else if (options.changelogProducer().equals(FULL_COMPACTION)) {
            result =
                    startingScanner.scan(
                            snapshotReader.withLevelFilter(
                                    level -> level == options.numLevels() - 1));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else {
            result = startingScanner.scan(snapshotReader);
        }

        if (result instanceof ScannedResult) {
            ScannedResult scannedResult = (ScannedResult) result;
            currentWatermark = scannedResult.currentWatermark();
            long currentSnapshotId = scannedResult.currentSnapshotId();
            nextSnapshotId = currentSnapshotId + 1;
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
            LOG.debug(
                    "Starting snapshot is {}, next snapshot will be {}.",
                    scannedResult.plan().snapshotId(),
                    nextSnapshotId);
            return scannedResult.plan();
        } else if (result instanceof StartingScanner.NextSnapshot) {
            // 接收到 NextSnapshot，获取到下一个应该开始等待的快照 ID
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                                    snapshotManager.snapshot(nextSnapshotId - 1));
            LOG.debug("There is no starting snapshot. Next snapshot will be {}.", nextSnapshotId);
        } else if (result instanceof StartingScanner.NoSnapshot) {
            LOG.debug("There is no starting snapshot and currently there is no next snapshot.");
        }
        // 对于 NextSnapshot 和 NoSnapshot 的情况，本次规划都返回一个空计划，任务在当前checkpoint周期内不会收到任何数据，处于空闲等待状态
        // 在下一个调度周期，此时NextSnapshot指向的nextSnapshotId快照已经生成，再次调用plan()时，因nextSnapshotId非空直接进入nextPlan()
        return SnapshotNotExistPlan.INSTANCE;
    }

    /**
     * 这是一个无限循环，核心逻辑是：
     * 1.使用 nextSnapshotProvider 不断地尝试获取 nextSnapshotId 对应的快照。
     * 2.如果获取不到（snapshot == null），说明暂时没有新数据，返回空计划 SnapshotNotExistPlan.INSTANCE，等待下次被调用。
     * 3.如果获取到了新的 snapshot： a. 用 boundedChecker 检查是否达到结束条件。 b. 调用 followUpScanner.scan() 来扫描这个快照，生成 Plan。 c. 将 nextSnapshotId 加一，为下一次扫描做准备。 d. 返回生成的 Plan
     */
    private Plan nextPlan() {
        while (true) {
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }

            Snapshot snapshot = nextSnapshotProvider.getNextSnapshot(nextSnapshotId);
            if (snapshot == null) {
                return SnapshotNotExistPlan.INSTANCE;
            }

            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            if (shouldDelaySnapshot(snapshot)) {
                return SnapshotNotExistPlan.INSTANCE;
            }

            // first try to get overwrite changes
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE) {
                SnapshotReader.Plan overwritePlan = handleOverwriteSnapshot(snapshot);
                if (overwritePlan != null) {
                    nextSnapshotId++;
                    if (overwritePlan.splits().isEmpty()) {
                        continue;
                    }
                    return overwritePlan;
                }
            }

            if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan plan = followUpScanner.scan(snapshot, snapshotReader);
                currentWatermark = plan.watermark();
                nextSnapshotId++;
                if (plan.splits().isEmpty()) {
                    continue;
                }
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private boolean shouldDelaySnapshot(Snapshot snapshot) {
        if (scanDelayMillis == null) {
            return false;
        }

        long snapshotMills = System.currentTimeMillis() - scanDelayMillis;
        return snapshot.timeMillis() > snapshotMills;
    }

    @Nullable
    protected SnapshotReader.Plan handleOverwriteSnapshot(Snapshot snapshot) {
        if (supportStreamingReadOverwrite) {
            LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
            SnapshotReader.Plan overwritePlan =
                    followUpScanner.getOverwriteChangesPlan(snapshot, snapshotReader, !hasPk);
            currentWatermark = overwritePlan.watermark();
            return overwritePlan;
        }
        return null;
    }

    protected FollowUpScanner createFollowUpScanner() {
        switch (scanMode) {
            case COMPACT_BUCKET_TABLE:
                return new DeltaFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
        }

        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
            case FULL_COMPACTION:
            case LOOKUP:
                followUpScanner = new ChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }
        return followUpScanner;
    }

    protected BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    private Long getScanDelayMillis() {
        return options.streamingReadDelay() == null
                ? null
                : options.streamingReadDelay().toMillis();
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Nullable
    @Override
    public Long watermark() {
        return currentWatermark;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
        if (nextSnapshotId != null && scanAllSnapshot) {
            startingScanner =
                    new StaticFromSnapshotStartingScanner(snapshotManager, nextSnapshotId);
            restore(null);
        } else {
            restore(nextSnapshotId);
        }
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            snapshotReader.consumerManager().resetConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
