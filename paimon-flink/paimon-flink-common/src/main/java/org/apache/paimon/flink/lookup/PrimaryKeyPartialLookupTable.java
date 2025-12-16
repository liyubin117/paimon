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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ProjectedRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.table.BucketMode.POSTPONE_BUCKET;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lookup table for primary key which supports to read the LSM tree directly.
 * 当配置 lookup.cache (即 LOOKUP_CACHE_MODE) 为 AUTO (默认值)，并且维表 JOIN 的键 ( joinKeys ) 与维表的主键 ( table.primaryKeys() ) 完全一致时
 * 根据上游流数据 Join Key 的值，计算出这个 Key 对应 Paimon 表中的哪个 Bucket，然后只加载该 Bucket 的数据
 * 这样，每个 Flink 的 Lookup 并发实例就不再需要加载维表的全量数据，而只需要负责一部分 Bucket 的数据，极大地降低了单个节点的内存消耗和启动时间。
 * */
public class PrimaryKeyPartialLookupTable implements LookupTable {

    private final QueryExecutorFactory executorFactory;
    @Nullable private final ProjectedRow keyRearrange;
    @Nullable private final ProjectedRow trimmedKeyRearrange;

    private Predicate specificPartition;
    @Nullable private Filter<InternalRow> cacheRowFilter;
    private QueryExecutor queryExecutor;

    private final Projection partitionFromPk;
    private final Projection bucketKeyFromPk;
    private final BucketFunction bucketFunction;

    private PrimaryKeyPartialLookupTable(
            QueryExecutorFactory executorFactory, FileStoreTable table, List<String> joinKey) {
        this.executorFactory = executorFactory;
        BucketMode bucketMode = table.bucketMode();
        if (bucketMode != BucketMode.HASH_FIXED && bucketMode != BucketMode.POSTPONE_MODE) {
            throw new UnsupportedOperationException(
                    "Unsupported mode for partial lookup: " + bucketMode);
        }

        CoreOptions coreOptions = CoreOptions.fromMap(table.options());

        if (!coreOptions.needLookup()
                && coreOptions.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new UnsupportedOperationException(
                    "Only support deduplicate merge engine when table does not need lookup, but merge engine is:  "
                            + coreOptions.mergeEngine());
        }

        if (coreOptions.mergeEngine() == CoreOptions.MergeEngine.DEDUPLICATE
                && !coreOptions.sequenceField().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Unsupported sequence fields definition for partial lookup when use deduplicate merge engine, but sequence fields are:  "
                            + coreOptions.sequenceField());
        }

        TableSchema schema = table.schema();
        this.partitionFromPk =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.partitionKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());
        this.bucketKeyFromPk =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.bucketKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());

        ProjectedRow keyRearrange = null;
        if (!table.primaryKeys().equals(joinKey)) {
            keyRearrange =
                    ProjectedRow.from(
                            table.primaryKeys().stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.keyRearrange = keyRearrange;

        List<String> trimmedPrimaryKeys = schema.trimmedPrimaryKeys();
        ProjectedRow trimmedKeyRearrange = null;
        if (!trimmedPrimaryKeys.equals(joinKey)) {
            trimmedKeyRearrange =
                    ProjectedRow.from(
                            trimmedPrimaryKeys.stream()
                                    .map(joinKey::indexOf)
                                    .mapToInt(value -> value)
                                    .toArray());
        }
        this.trimmedKeyRearrange = trimmedKeyRearrange;
        this.bucketFunction =
                BucketFunction.create(
                        new CoreOptions(schema.options()), schema.logicalBucketKeyType());
    }

    @VisibleForTesting
    QueryExecutor queryExecutor() {
        return queryExecutor;
    }

    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    @Override
    public void open() throws Exception {
        this.queryExecutor = executorFactory.create(specificPartition, cacheRowFilter);
        refresh();
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        InternalRow adjustedKey = key;
        if (keyRearrange != null) {
            adjustedKey = keyRearrange.replaceRow(adjustedKey);
        }

        BinaryRow partition = partitionFromPk.apply(adjustedKey);
        Integer numBuckets = queryExecutor.numBuckets(partition);
        if (numBuckets == null) {
            // no data, just return none
            return Collections.emptyList();
        }
        int bucket = bucket(numBuckets, adjustedKey);

        InternalRow trimmedKey = key;
        if (trimmedKeyRearrange != null) {
            trimmedKey = trimmedKeyRearrange.replaceRow(trimmedKey);
        }

        InternalRow kv = queryExecutor.lookup(partition, bucket, trimmedKey);
        if (kv == null) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(kv);
        }
    }

    private int bucket(int numBuckets, InternalRow primaryKey) {
        BinaryRow bucketKey = bucketKeyFromPk.apply(primaryKey);
        return bucketFunction.bucket(bucketKey, numBuckets);
    }

    @Override
    public void refresh() {
        queryExecutor.refresh(); // 调用LocalQueryExecutor#refresh
    }

    @Override
    public void specifyCacheRowFilter(Filter<InternalRow> filter) {
        this.cacheRowFilter = filter;
    }

    @Override
    public void close() throws IOException {
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    public static PrimaryKeyPartialLookupTable createLocalTable(
            FileStoreTable table,
            int[] projection,
            File tempPath,
            List<String> joinKey,
            Set<Integer> requireCachedBucketIds) {
        return new PrimaryKeyPartialLookupTable(
                (filter, cacheRowFilter) ->
                        new LocalQueryExecutor( // 调用生成LocalQueryExecutor
                                new LookupFileStoreTable(table, joinKey),
                                projection,
                                tempPath,
                                filter,
                                requireCachedBucketIds,
                                cacheRowFilter),
                table,
                joinKey);
    }

    public static PrimaryKeyPartialLookupTable createRemoteTable(
            FileStoreTable table, int[] projection, List<String> joinKey) {
        return new PrimaryKeyPartialLookupTable(
                (filter, cacheRowFilter) -> new RemoteQueryExecutor(table, projection),
                table,
                joinKey);
    }

    interface QueryExecutorFactory {
        QueryExecutor create(Predicate filter, @Nullable Filter<InternalRow> cacheRowFilter);
    }

    interface QueryExecutor extends Closeable {

        @Nullable
        Integer numBuckets(BinaryRow partition);

        InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException;

        void refresh();
    }

    static class LocalQueryExecutor implements QueryExecutor {

        private static final Logger LOG = LoggerFactory.getLogger(LocalQueryExecutor.class);

        private final LocalTableQuery tableQuery; // LocalQueryExecutor有成员变量LocalTableQuery
        private final StreamTableScan scan;
        private final String tableName;

        private final Integer defaultNumBuckets;
        private final Map<BinaryRow, Integer> numBuckets;

        private LocalQueryExecutor(
                FileStoreTable table,
                int[] projection,
                File tempPath,
                @Nullable Predicate filter,
                Set<Integer> requireCachedBucketIds,
                @Nullable Filter<InternalRow> cacheRowFilter) {
            this.tableQuery =
                    table.newLocalTableQuery()
                            .withValueProjection(projection)
                            .withIOManager(new IOManagerImpl(tempPath.toString()));

            if (cacheRowFilter != null) {
                this.tableQuery.withCacheRowFilter(cacheRowFilter);
            }

            this.scan =
                    table.newReadBuilder()
                            .dropStats()
                            .withFilter(filter)
                            .withBucketFilter(
                                    requireCachedBucketIds == null
                                            ? null
                                            : requireCachedBucketIds::contains)
                            .newStreamScan(); // 实际调用 AbstractFileStoreTable#newStreamScan

            this.tableName = table.name();
            this.defaultNumBuckets = table.bucketSpec().getNumBuckets();
            this.numBuckets = new HashMap<>();
        }

        @Override
        @Nullable
        public Integer numBuckets(BinaryRow partition) {
            return numBuckets.get(partition);
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        /**
         * 不停刷新元数据缓存，作用：
         * 同步元数据：refresh() 的核心目的是确保 LocalTableQuery 持有的数据文件元数据（即 DataFileMeta 列表）与 Paimon 表的最新状态保持同步。
         * 感知数据变更：当 Paimon 表发生数据写入、合并、过期等操作导致数据文件布局变化时，StreamTableScan 在下一次 plan() 时会反映这些变化，生成新的 DataSplit。refresh() 通过处理这些新的 DataSplit，使得 LocalTableQuery 能够感知到这些变化。
         * 保证查找准确性：如果 LocalTableQuery 的元数据不是最新的，那么查找操作可能会在已经不存在的文件中查找，或者遗漏新写入的数据文件，导致查找结果不正确或不完整。refresh() 通过更新 DataFileMeta 信息，保证了后续 lookup 操作的准确性。
         * 只是刷新元数据，当真正需要访问这些文件进行查找时，lookup file才会被按需创建。
         */
        @Override
        public void refresh() {
            while (true) {
                List<Split> splits = scan.plan().splits();
                log(splits);

                if (splits.isEmpty()) {
                    return;
                }

                for (Split split : splits) {
                    refreshSplit((DataSplit) split);
                }
            }
        }

        @VisibleForTesting
        void refreshSplit(DataSplit split) {
            BinaryRow partition = split.partition();
            int bucket = split.bucket();
            List<DataFileMeta> before = split.beforeFiles();
            List<DataFileMeta> after = split.dataFiles();

            tableQuery.refreshFiles(partition, bucket, before, after);
            Integer totalBuckets = split.totalBuckets();
            if (totalBuckets == null) {
                // Just for compatibility with older versions
                checkArgument(
                        defaultNumBuckets > 0,
                        "This is a bug, old version table numBuckets should be greater than 0.");
                totalBuckets = defaultNumBuckets;
            }
            numBuckets.put(partition, totalBuckets);
        }

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }

        private void log(List<Split> splits) {
            if (splits.isEmpty()) {
                LOG.info("LocalQueryExecutor didn't get splits from {}.", tableName);
                return;
            }

            DataSplit dataSplit = (DataSplit) splits.get(0);
            LOG.info(
                    "LocalQueryExecutor get splits from {} with snapshotId {}.",
                    tableName,
                    dataSplit.snapshotId());
        }
    }

    /**
     * Query service执行器
     * 为了解决本地磁盘（尤其是 HDD）随机读性能差的问题，Paimon 借鉴了 "服务化" 的思想，推出了 Query Service
     * 可以为一张 Paimon 表启动一个独立的、常驻的 Flink 作业作为查询服务。这个服务可以部署在拥有高性能 SSD 的专用节点上，它预先加载数据并对外提供点查服务。
     * 当维表 Join 任务运行时，它会优先连接这个 Query Service 来获取数据，从而将 I/O 压力从计算节点转移到专用的服务节点上，实现了计算与存储的物理分离，保证了 Lookup 性能的稳定性和高效性
     */
    static class RemoteQueryExecutor implements QueryExecutor {

        private final RemoteTableQuery tableQuery;
        private final Integer numBuckets;

        private RemoteQueryExecutor(FileStoreTable table, int[] projection) {
            this.tableQuery = new RemoteTableQuery(table).withValueProjection(projection);
            int numBuckets = table.bucketSpec().getNumBuckets();
            if (numBuckets == POSTPONE_BUCKET) {
                throw new UnsupportedOperationException(
                        "Remote query does not support POSTPONE_BUCKET.");
            }
            this.numBuckets = numBuckets;
        }

        @Override
        @Nullable
        public Integer numBuckets(BinaryRow partition) {
            return numBuckets;
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        @Override
        public void refresh() {}

        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }
}
