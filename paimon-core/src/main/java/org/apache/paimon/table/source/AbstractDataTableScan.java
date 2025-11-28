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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotFullStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.paimon.table.source.snapshot.EmptyResultStartingScanner;
import org.apache.paimon.table.source.snapshot.FileCreationTimeStartingScanner;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalDeltaStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalDiffStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTagStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.IncrementalBetweenScanMode.DIFF;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation.
 * 扮演了一个“扫描任务协调者”的角色。它承接上层的扫描需求（通过 CoreOptions 和 with 方法），然后创建出具体的执行策略（StartingScanner），为后续真正的数据读取做准备
 *
 * 提供扫描配置的统一入口：它提供了一系列 with... 方法（如 withBucket, withPartitionFilter 等），允许上层调用者以链式调用的方式方便地设置各种扫描条件和过滤器。这为不同类型的扫描（批处理、流处理）提供了统一的配置接口。
 * 作为扫描策略的工厂：其最核心的功能是根据用户在 CoreOptions 中设置的参数（例如 startup-mode, scan.snapshot-id 等），决定并创建出合适的起始扫描器 (StartingScanner)。StartingScanner 定义了从哪个快照（Snapshot）开始以及如何扫描数据。
 * 抽象和封装底层细节：它封装了与 SnapshotReader 的交互细节。调用者不需要直接与 SnapshotReader 打交道，只需通过 AbstractDataTableScan 配置扫描即可。如 Javadoc 中所述，它是 FileStoreScan 之上的一个抽象层，用于提供输入切片（input split）的生成。
 * */
abstract class AbstractDataTableScan implements DataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataTableScan.class);

    protected final TableSchema schema;
    private final CoreOptions options; // 保存了所有与表相关的配置，是决定扫描行为的依据
    protected final SnapshotReader snapshotReader; // 实际执行快照读取和文件过滤的组件
    private final TableQueryAuth queryAuth;

    @Nullable private RowType readType;

    protected AbstractDataTableScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        this.schema = schema;
        this.options = options;
        this.snapshotReader = snapshotReader;
        this.queryAuth = queryAuth;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        snapshotReader.withFilter(predicate);
        return this;
    }

    @Override
    public AbstractDataTableScan withBucket(int bucket) {
        snapshotReader.withBucket(bucket);
        return this;
    }

    @Override
    public AbstractDataTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        snapshotReader.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public InnerTableScan withReadType(@Nullable RowType readType) {
        this.readType = readType;
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        snapshotReader.withPartitionFilter(partitionSpec);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(List<BinaryRow> partitions) {
        snapshotReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        snapshotReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        snapshotReader.withPartitionFilter(partitionPredicate);
        return this;
    }

    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotReader.withLevelFilter(levelFilter);
        return this;
    }

    public AbstractDataTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
        snapshotReader.withMetricRegistry(metricsRegistry);
        return this;
    }

    protected void authQuery() {
        if (!options.queryAuthEnabled()) {
            return;
        }
        queryAuth.auth(readType == null ? null : readType.getFieldNames());
        // TODO add support for row level access control
    }

    @Override
    public AbstractDataTableScan dropStats() {
        snapshotReader.dropStats();
        return this;
    }

    public CoreOptions options() {
        return options;
    }

    /**
     * 创建扫描策略
     * 根据 isStreaming 参数（判断是流模式还是批模式）以及 CoreOptions 中的配置，通过一个巨大的 switch 语句来创建不同的 StartingScanner 实例
     *
     * 1.判断扫描模式：首先会检查 stream-scan-mode，处理一些特殊的流式扫描，如 streaming-compact。
     * 2.检查消费者ID：在流模式下，会尝试从 ConsumerManager 读取已保存的消费位点。
     * 3.根据 startup-mode 创建扫描器：这是主要逻辑，不同的启动模式对应不同的 StartingScanner 实现。
     *      LATEST_FULL: 从最新的快照进行全量扫描。
     *      FROM_TIMESTAMP: 从指定的时间戳开始扫描。
     *      FROM_SNAPSHOT: 从指定的快照ID开始扫描。
     *      INCREMENTAL: 增量扫描，会调用 createIncrementalStartingScanner 方法处理更复杂的增量逻辑。
     *      等等...
     */
    protected StartingScanner createStartingScanner(boolean isStreaming) {
        SnapshotManager snapshotManager = snapshotReader.snapshotManager();
        ChangelogManager changelogManager = snapshotReader.changelogManager();
        CoreOptions.StreamScanMode type =
                options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                checkArgument(
                        isStreaming, "Set 'streaming-compact' in batch mode. This is unexpected.");
                return new ContinuousCompactorStartingScanner(snapshotManager);
            case FILE_MONITOR:
                return new FullStartingScanner(snapshotManager);
        }

        // read from consumer id
        String consumerId = options.consumerId();
        if (isStreaming && consumerId != null && !options.consumerIgnoreProgress()) {
            ConsumerManager consumerManager = snapshotReader.consumerManager();
            Optional<Consumer> consumer = consumerManager.consumer(consumerId);
            if (consumer.isPresent()) {
                return new ContinuousFromSnapshotStartingScanner(
                        snapshotManager,
                        changelogManager,
                        consumer.get().nextSnapshot(),
                        options.changelogLifecycleDecoupled());
            }
        }

        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner(snapshotManager);
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner(snapshotManager)
                        : new FullStartingScanner(snapshotManager);
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits =
                            options.toConfiguration()
                                    .getOptional(FULL_COMPACTION_DELTA_COMMITS)
                                    .orElse(1);
                    return new FullCompactedStartingScanner(snapshotManager, deltaCommits);
                } else {
                    return new CompactedStartingScanner(snapshotManager);
                }
            case FROM_TIMESTAMP:
                String timestampStr = options.scanTimestamp();
                Long startupMillis = options.scanTimestampMills();
                if (startupMillis == null && timestampStr != null) {
                    startupMillis =
                            DateTimeUtils.parseTimestampData(timestampStr, 3, TimeZone.getDefault())
                                    .getMillisecond();
                }
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(
                                snapshotManager,
                                changelogManager,
                                startupMillis,
                                options.changelogLifecycleDecoupled())
                        : new StaticFromTimestampStartingScanner(snapshotManager, startupMillis);
            case FROM_FILE_CREATION_TIME:
                Long fileCreationTimeMills = options.scanFileCreationTimeMills();
                return new FileCreationTimeStartingScanner(snapshotManager, fileCreationTimeMills);
            case FROM_CREATION_TIMESTAMP:
                Long creationTimeMills = options.scanCreationTimeMills();
                return createCreationTimestampStartingScanner(
                        snapshotManager,
                        changelogManager,
                        creationTimeMills,
                        options.changelogLifecycleDecoupled(),
                        isStreaming);

            case FROM_SNAPSHOT:
                if (options.scanSnapshotId() != null) {
                    return isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                                    snapshotManager,
                                    changelogManager,
                                    options.scanSnapshotId(),
                                    options.changelogLifecycleDecoupled())
                            : new StaticFromSnapshotStartingScanner(
                                    snapshotManager, options.scanSnapshotId());
                } else if (options.scanWatermark() != null) {
                    checkArgument(!isStreaming, "Cannot scan from watermark in streaming mode.");
                    return new StaticFromWatermarkStartingScanner(
                            snapshotManager, options().scanWatermark());
                } else if (options.scanTagName() != null) {
                    checkArgument(!isStreaming, "Cannot scan from tag in streaming mode.");
                    return new StaticFromTagStartingScanner(
                            snapshotManager, options().scanTagName());
                } else {
                    throw new UnsupportedOperationException("Unknown snapshot read mode");
                }
            case FROM_SNAPSHOT_FULL:
                Long scanSnapshotId = options.scanSnapshotId();
                checkNotNull(
                        scanSnapshotId,
                        "scan.snapshot-id must be set when startupMode is FROM_SNAPSHOT_FULL.");
                return isStreaming
                        ? new ContinuousFromSnapshotFullStartingScanner(
                                snapshotManager, scanSnapshotId)
                        : new StaticFromSnapshotStartingScanner(snapshotManager, scanSnapshotId);
            case INCREMENTAL:
                checkArgument(!isStreaming, "Cannot read incremental in streaming mode.");
                return createIncrementalStartingScanner(snapshotManager);
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }

    public static StartingScanner createCreationTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long creationMillis,
            boolean changelogDecoupled,
            boolean isStreaming) {
        Long startingSnapshotPrevId =
                TimeTravelUtil.earlierThanTimeMills(
                        snapshotManager,
                        changelogManager,
                        creationMillis,
                        changelogDecoupled,
                        true);
        final StartingScanner scanner;
        Optional<Long> startingSnapshotId =
                Optional.ofNullable(startingSnapshotPrevId)
                        .map(id -> id + 1)
                        .filter(
                                id ->
                                        snapshotManager.snapshotExists(id)
                                                || changelogManager.longLivedChangelogExists(id));
        if (startingSnapshotId.isPresent()) {
            scanner =
                    isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                                    snapshotManager,
                                    changelogManager,
                                    startingSnapshotId.get(),
                                    changelogDecoupled)
                            : new StaticFromSnapshotStartingScanner(
                                    snapshotManager, startingSnapshotId.get());
        } else {
            scanner = new FileCreationTimeStartingScanner(snapshotManager, creationMillis);
        }
        return scanner;
    }

    private StartingScanner createIncrementalStartingScanner(SnapshotManager snapshotManager) {
        Options conf = options.toConfiguration();

        if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN)) {
            Pair<String, String> incrementalBetween = options.incrementalBetween();

            TagManager tagManager =
                    new TagManager(
                            snapshotManager.fileIO(),
                            snapshotManager.tablePath(),
                            snapshotManager.branch());
            Optional<Tag> startTag = tagManager.get(incrementalBetween.getLeft());
            Optional<Tag> endTag = tagManager.get(incrementalBetween.getRight());

            if (startTag.isPresent() && endTag.isPresent()) {
                return IncrementalDiffStartingScanner.betweenTags(
                        startTag.get(), endTag.get(), snapshotManager, incrementalBetween);
            } else {
                long startId, endId;
                try {
                    startId = Long.parseLong(incrementalBetween.getLeft());
                    endId = Long.parseLong(incrementalBetween.getRight());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Didn't find two tags for start '%s' and end '%s', and they are not two snapshot Ids. "
                                            + "Please set two tags or two snapshot Ids.",
                                    incrementalBetween.getLeft(), incrementalBetween.getRight()));
                }

                checkArgument(
                        endId >= startId,
                        "Ending snapshotId should >= starting snapshotId %s.",
                        endId,
                        startId);

                if (snapshotManager.earliestSnapshot() == null) {
                    LOG.warn("There is currently no snapshot. Waiting for snapshot generation.");
                    return new EmptyResultStartingScanner(snapshotManager);
                }

                if (startId == endId) {
                    return new EmptyResultStartingScanner(snapshotManager);
                }

                CoreOptions.IncrementalBetweenScanMode scanMode =
                        options.incrementalBetweenScanMode();
                return scanMode == DIFF
                        ? IncrementalDiffStartingScanner.betweenSnapshotIds(
                                startId, endId, snapshotManager)
                        : IncrementalDeltaStartingScanner.betweenSnapshotIds(
                                startId, endId, snapshotManager, toSnapshotScanMode(scanMode));
            }
        } else if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)) {
            String incrementalBetweenStr = options.incrementalBetweenTimestamp();
            String[] split = incrementalBetweenStr.split(",");
            if (split.length != 2) {
                throw new IllegalArgumentException(
                        "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: "
                                + incrementalBetweenStr);
            }

            Pair<Long, Long> incrementalBetween;
            try {
                incrementalBetween = Pair.of(Long.parseLong(split[0]), Long.parseLong(split[1]));
            } catch (NumberFormatException nfe) {
                try {
                    long startTimestamp =
                            DateTimeUtils.parseTimestampData(split[0], 3, TimeZone.getDefault())
                                    .getMillisecond();
                    long endTimestamp =
                            DateTimeUtils.parseTimestampData(split[1], 3, TimeZone.getDefault())
                                    .getMillisecond();
                    incrementalBetween = Pair.of(startTimestamp, endTimestamp);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: "
                                    + incrementalBetweenStr);
                }
            }

            Snapshot earliestSnapshot = snapshotManager.earliestSnapshot();
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            if (earliestSnapshot == null || latestSnapshot == null) {
                return new EmptyResultStartingScanner(snapshotManager);
            }

            long startTimestamp = incrementalBetween.getLeft();
            long endTimestamp = incrementalBetween.getRight();
            checkArgument(
                    endTimestamp >= startTimestamp,
                    "Ending timestamp %s should be >= starting timestamp %s.",
                    endTimestamp,
                    startTimestamp);

            if (startTimestamp == endTimestamp
                    || startTimestamp > latestSnapshot.timeMillis()
                    || endTimestamp < earliestSnapshot.timeMillis()) {
                return new EmptyResultStartingScanner(snapshotManager);
            }

            CoreOptions.IncrementalBetweenScanMode scanMode = options.incrementalBetweenScanMode();

            return scanMode == DIFF
                    ? IncrementalDiffStartingScanner.betweenTimestamps(
                            startTimestamp, endTimestamp, snapshotManager)
                    : IncrementalDeltaStartingScanner.betweenTimestamps(
                            startTimestamp,
                            endTimestamp,
                            snapshotManager,
                            toSnapshotScanMode(scanMode));
        } else if (conf.contains(CoreOptions.INCREMENTAL_TO_AUTO_TAG)) {
            String endTag = options.incrementalToAutoTag();
            return IncrementalDiffStartingScanner.toEndAutoTag(snapshotManager, endTag, options);
        } else {
            throw new UnsupportedOperationException("Unknown incremental read mode.");
        }
    }

    private ScanMode toSnapshotScanMode(CoreOptions.IncrementalBetweenScanMode scanMode) {
        switch (scanMode) {
            case AUTO:
                return options.changelogProducer() == ChangelogProducer.NONE
                        ? ScanMode.DELTA
                        : ScanMode.CHANGELOG;
            case DELTA:
                return ScanMode.DELTA;
            case CHANGELOG:
                return ScanMode.CHANGELOG;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported incremental scan mode " + scanMode.name());
        }
    }
}
