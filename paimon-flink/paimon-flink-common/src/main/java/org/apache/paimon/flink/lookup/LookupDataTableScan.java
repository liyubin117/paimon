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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.StartupMode;
import static org.apache.paimon.flink.lookup.LookupFileStoreTable.LookupStreamScanMode;

/**
 * {@link org.apache.paimon.table.source.StreamTableScan} implementation for lookup streaming
 * planning.
 * 是一个为 Flink 的 Lookup Join 算子提供一个可供查询和持续更新的本地缓存。它负责在后台悄悄地同步维表的最新数据
 * 在 Lookup Join 中，Paimon 表通常作为维表（Dimension Table）。这个 Scan 的主要目标是：
 *      首次加载：在作业启动时，高效地读取维表的全量或部分数据，填充到 Flink 的 Lookup Cache 中。
 *      变更捕获：持续地消费维表的变更数据（CDC），并用这些变更来更新 Lookup Cache，确保维表数据是最新的。
 * 它通过继承 DataTableStreamScan 复用了大部分的流式扫描和状态管理逻辑，但会根据 Lookup Join 的特定需求调整其行为，例如它可能会有特殊的 lookupScanMode 来控制如何生成变更流。
 *
 */
public class LookupDataTableScan extends DataTableStreamScan {

    private static final Logger LOG = LoggerFactory.getLogger(LookupDataTableScan.class);

    private final StartupMode startupMode;
    private final LookupStreamScanMode lookupScanMode;

    public LookupDataTableScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean supportStreamingReadOverwrite,
            LookupStreamScanMode lookupScanMode,
            TableQueryAuth queryAuth,
            boolean hasPk) {
        super(
                schema,
                options,
                snapshotReader,
                snapshotManager,
                changelogManager,
                supportStreamingReadOverwrite,
                queryAuth,
                hasPk);

        this.startupMode = options.startupMode();
        this.lookupScanMode = lookupScanMode;
        dropStats();

        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    /**
     * 当维表发生了 OVERWRITE 这种破坏性操作时，简单地增量更新缓存可能会出错。
     * 如果父类无法处理这个 OVERWRITE（比如没有开启 streaming-read-overwrite），它不会像普通流一样报错或跳过，而是直接抛出一个自定义的 ReopenException。
     * 这个异常会被上层的 Lookup 算子捕获，触发整个维表缓存的重新加载 (re-load)，以确保数据的一致性。这是一种针对维表场景的、更健壮的容错机制
     */
    @Override
    @Nullable
    protected SnapshotReader.Plan handleOverwriteSnapshot(Snapshot snapshot) {
        SnapshotReader.Plan plan = super.handleOverwriteSnapshot(snapshot);
        if (plan != null) {
            return plan;
        }
        LOG.info("Dim table found OVERWRITE snapshot {}, reopen.", snapshot.id());
        throw new ReopenException();
    }

    @Override
    protected StartingScanner createStartingScanner(boolean isStreaming) {
        return startupMode != CoreOptions.StartupMode.COMPACTED_FULL
                ? new FullStartingScanner(snapshotReader.snapshotManager()) // 维表关联通常需要在启动时加载一个全量的维表快照到内存或本地磁盘作为基础缓存
                : super.createStartingScanner(isStreaming);
    }

    @Override
    protected FollowUpScanner createFollowUpScanner() {
        /**
         * 根据维表的特性选择不同的增量更新策略：
         * CHANGELOG: 沿用父类的逻辑，通过 changelog 或 delta 文件来更新缓存。这是最通用的方式。
         * FILE_MONITOR: 使用 AllDeltaFollowUpScanner，它会扫描所有的增量文件。这在某些特定场景下（如 lookup.cache-mode = 'AUTO'）更优。
         * COMPACT_DELTA_MONITOR: 一种更高效的模式，它只关心 Compaction 前后的数据差异，可以大大减少需要同步的数据量
         */
        switch (lookupScanMode) {
            case CHANGELOG:
                return super.createFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
            case COMPACT_DELTA_MONITOR:
                return new CompactionDiffFollowUpScanner();
            default:
                throw new UnsupportedOperationException(
                        "Unknown lookup stream scan mode: " + lookupScanMode.name());
        }
    }

    @Override
    protected BoundedChecker createBoundedChecker() {
        return BoundedChecker.neverEnd(); // 维表通常被认为是无界 (unbounded) 的。只要事实流还在，维表就应该持续提供服务并接收更新。因此，这里直接返回一个 BoundedChecker.neverEnd()，明确表示这个流永远不会自动结束
    }
}
