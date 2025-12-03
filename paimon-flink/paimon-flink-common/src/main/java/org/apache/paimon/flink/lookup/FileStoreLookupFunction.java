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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.lookup.partitioner.ShuffleStrategy;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST;
import static org.apache.paimon.flink.query.RemoteTableQuery.isRemoteServiceAvailable;
import static org.apache.paimon.lookup.rocksdb.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.lookup.rocksdb.RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** A lookup {@link TableFunction} for file store.
 * 是执行维表查找逻辑的主要类。它本身虽然没有直接实现 Flink 的 TableFunction 接口，但它被具体的 Flink 版本相关的包装类所使用：
 *  OldLookupFunction (用于 Flink 1.15 1.16): 继承自 org.apache.flink.table.functions.TableFunction
 *  NewLookupFunction (用于 Flink 1.17+): 继承自 org.apache.flink.table.functions.LookupFunction
 *
 *  依赖LookupTable完成实际的维表查找和缓存管理
 *
 * 核心职责:
 *  生命周期管理: 在 open() 方法中初始化维表数据（通过 LookupTable），在 close() 方法中释放资源。
 *  数据查找: 在 lookup() 方法中接收流数据中的关联键，并调用 LookupTable 进行查找。
 *  缓存刷新: 通过 tryRefresh() 方法管理维表缓存的刷新逻辑。
 * */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    private final FileStoreTable table; // 维表
    @Nullable private final PartitionLoader partitionLoader; // 用于处理动态分区scan.partitions
    private final List<String> projectFields;
    private final List<String> joinKeys;
    @Nullable private final Predicate predicate;
    @Nullable private final RefreshBlacklist refreshBlacklist;
    @Nullable private final ShuffleStrategy strategy;

    private final List<InternalRow.FieldGetter> projectFieldsGetters;

    private transient File path;
    private transient LookupTable lookupTable; // 实际执行查找和缓存的组件

    // interval of refreshing lookup table 缓存刷新间隔
    private transient Duration refreshInterval;
    // timestamp when refreshing lookup table 下一次刷新的时间戳
    private transient long nextRefreshTime;

    protected FunctionContext functionContext;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public FileStoreLookupFunction(
            FileStoreTable table,
            int[] projection,
            int[] joinKeyIndex,
            @Nullable Predicate predicate,
            @Nullable ShuffleStrategy strategy) {
        if (!TableScanUtils.supportCompactDiffStreamingReading(table)) {
            TableScanUtils.streamingReadingValidate(table);
        }

        this.table = table;
        this.partitionLoader = PartitionLoader.of(table);

        // join keys are based on projection fields
        RowType rowType = table.rowType();
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> rowType.getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> rowType.getFieldNames().get(i))
                        .collect(Collectors.toList());

        this.projectFieldsGetters =
                Arrays.stream(projection)
                        .mapToObj(i -> InternalRow.createFieldGetter(rowType.getTypeAt(i), i))
                        .collect(Collectors.toList());

        // add primary keys
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        if (partitionLoader != null) {
            partitionLoader.addPartitionKeysTo(joinKeys, projectFields);
        }

        this.predicate = predicate;

        this.refreshBlacklist =
                RefreshBlacklist.create(
                        table.options().get(LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST.key()));

        this.strategy = strategy;
    }

    // open()入口方法，被LookupFunction调用
    public void open(FunctionContext context) throws Exception {
        this.functionContext = context;
        String tmpDirectory = getTmpDirectory(context); // 获取临时目录
        open(tmpDirectory);
    }

    // we tag this method friendly for testing
    void open(String tmpDirectory) throws Exception {
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());
        if (!path.mkdirs()) {
            throw new RuntimeException("Failed to create dir: " + path);
        }
        open();
    }

    private void open() throws Exception {
        this.nextRefreshTime = -1;

        Options options = Options.fromMap(table.options());
        this.refreshInterval =
                options.getOptional(LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL)
                        .orElse(options.get(CONTINUOUS_DISCOVERY_INTERVAL));

        List<String> fieldNames = table.rowType().getFieldNames();
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();
        LOG.info(
                "lookup projection fields in lookup table:{}, join fields in lookup table:{}",
                projectFields,
                joinKeys);

        LOG.info("Creating lookup table for {}.", table.name());
        if (options.get(LOOKUP_CACHE_MODE) == LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            // 当表目录下有service/service-primary-key-lookup目录，说明启用了query service
            if (isRemoteServiceAvailable(table)) {
                this.lookupTable =
                        PrimaryKeyPartialLookupTable.createRemoteTable(table, projection, joinKeys);
                LOG.info(
                        "Remote service is available. Created PrimaryKeyPartialLookupTable with remote service.");
            } else {
                try {
                    // 当query service服务未启动时，优先启用partial cache，不会将整个维表加载到缓存中，而是在需要时根据主键去查询加载了对应bucket的缓存数据
                    this.lookupTable =
                            PrimaryKeyPartialLookupTable.createLocalTable(
                                    table, projection, path, joinKeys, getRequireCachedBucketIds());
                    LOG.info(
                            "Remote service isn't available. Created PrimaryKeyPartialLookupTable with LocalQueryExecutor.");
                } catch (UnsupportedOperationException e) {
                    LOG.info(
                            "Remote service isn't available. Cannot create PrimaryKeyPartialLookupTable with LocalQueryExecutor "
                                    + "because {}. Will create FullCacheLookupTable.",
                            e.getMessage());
                }
            }
        }

        if (lookupTable == null) { // 若前面条件都未满足，使用FullCacheLookupTable
            FullCacheLookupTable.Context context =
                    new FullCacheLookupTable.Context(
                            table,
                            projection,
                            predicate,
                            createProjectedPredicate(projection),
                            path,
                            joinKeys,
                            getRequireCachedBucketIds());
            this.lookupTable = FullCacheLookupTable.create(context, options.get(LOOKUP_CACHE_ROWS));
            LOG.info("Created {}.", lookupTable.getClass().getSimpleName());
        }

        if (partitionLoader != null) { // 如果设置了scan.partitions，打开 partitionLoader，检查并加载分区信息，然后调用 lookupTable.specificPartitionFilter()
            partitionLoader.open();
            partitionLoader.checkRefresh();
            List<BinaryRow> partitions = partitionLoader.partitions();
            if (!partitions.isEmpty()) {
                lookupTable.specificPartitionFilter(partitionLoader.createSpecificPartFilter());
            }
        }

        if (cacheRowFilter != null) { // 缓存的行过滤
            lookupTable.specifyCacheRowFilter(cacheRowFilter);
        }
        lookupTable.open(); // 触发实际的缓存加载
    }

    @Nullable
    private Predicate createProjectedPredicate(int[] projection) {
        Predicate adjustedPredicate = null;
        if (predicate != null) {
            // adjust to projection index
            adjustedPredicate =
                    transformFieldMapping(
                                    this.predicate,
                                    IntStream.range(0, table.rowType().getFieldCount())
                                            .map(i -> Ints.indexOf(projection, i))
                                            .toArray())
                            .orElse(null);
        }
        return adjustedPredicate;
    }

    /**
     * 当流数据到达需要进行维表关联的算子时，Flink 会调用包装类的 eval(...) (Old) 或 lookup(...) (New) 方法，这些方法内部会调用该方法
     */
    public Collection<RowData> lookup(RowData keyRow) {
        try {
            tryRefresh(); // 尝试刷新缓存

            if (LOG.isDebugEnabled()) {
                LOG.debug("lookup key:{}", keyRow.toString());
            }
            InternalRow key = new FlinkRowWrapper(keyRow);
            if (partitionLoader == null) { // 若没有指定scan.partitions，则直接用原始的key查找
                return lookupInternal(key);
            }

            if (partitionLoader.partitions().isEmpty()) {
                return Collections.emptyList();
            }
            // 如果 partitionLoader 存在且有分区数据，会遍历每个分区，将原始 key 与分区信息通过 JoinedRow.join(key, partition) 合并成新的 key，然后调用 lookupInternal
            List<RowData> rows = new ArrayList<>();
            for (BinaryRow partition : partitionLoader.partitions()) {
                rows.addAll(lookupInternal(JoinedRow.join(key, partition)));
            }
            return rows;
        } catch (OutOfRangeException | ReopenException e) {
            reopen();
            return lookup(keyRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<RowData> lookupInternal(InternalRow key) throws IOException {
        List<RowData> rows = new ArrayList<>();
        List<InternalRow> lookupResults = lookupTable.get(key); // 从缓存获取匹配的数据
        for (InternalRow matchedRow : lookupResults) {
            rows.add(new FlinkRowData(matchedRow));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "matched rows in lookup table, size:{}, rows:{}",
                    lookupResults.size(),
                    lookupResults.stream()
                            .map(row -> logRow(projectFieldsGetters, row))
                            .collect(Collectors.toList()));
        }

        return rows;
    }

    private void reopen() {
        try {
            close();
            open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void tryRefresh() throws Exception {
        // 1. check if this time is in black list 检查当前时间是否在刷新黑名单 (refreshBlacklist) 内
        if (refreshBlacklist != null && !refreshBlacklist.canRefresh()) {
            return;
        }

        // 2. refresh dynamic partition 如果 partitionLoader 存在，刷新动态分区
        if (partitionLoader != null) {
            boolean partitionChanged = partitionLoader.checkRefresh(); // 检查是否分区有变化
            List<BinaryRow> partitions = partitionLoader.partitions();
            if (partitions.isEmpty()) {
                // no data to be load, fast exit
                return;
            }

            if (partitionChanged) { // 如果分区发生变化
                // reopen with latest partition
                lookupTable.specificPartitionFilter(partitionLoader.createSpecificPartFilter()); // 更新分区过滤器
                lookupTable.close();
                lookupTable.open(); // 先close再open，重新加载缓存
                // no need to refresh the lookup table because it is reopened
                return;
            }
        }

        // 3. refresh lookup table
        if (shouldRefreshLookupTable()) {
            lookupTable.refresh(); // 对于主键表，调用 PrimaryKeyPartialLookupTable#refresh
            nextRefreshTime = System.currentTimeMillis() + refreshInterval.toMillis();
        }
    }

    private boolean shouldRefreshLookupTable() {
        if (nextRefreshTime > System.currentTimeMillis()) {
            return false;
        }

        if (nextRefreshTime > 0) {
            LOG.info(
                    "Lookup table {} has refreshed after {} second(s), refreshing",
                    table.name(),
                    refreshInterval.toMillis() / 1000);
        }
        return true;
    }

    @VisibleForTesting
    LookupTable lookupTable() {
        return lookupTable;
    }

    @VisibleForTesting
    long nextBlacklistCheckTime() {
        return refreshBlacklist == null ? -1 : refreshBlacklist.nextBlacklistCheckTime();
    }

    // 释放 LookupTable 持有的资源（如 RocksDB 实例、文件句柄、线程池等）；将lookupTable置空；删除本地临时文件目录
    @Override
    public void close() throws IOException {
        if (lookupTable != null) {
            lookupTable.close();
            lookupTable = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static String getTmpDirectory(FunctionContext context) {
        try {
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    extractStreamingRuntimeContext(field.get(context));
            String[] tmpDirectories =
                    runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamingRuntimeContext extractStreamingRuntimeContext(Object runtimeContext)
            throws NoSuchFieldException, IllegalAccessException {
        if (runtimeContext instanceof StreamingRuntimeContext) {
            return (StreamingRuntimeContext) runtimeContext;
        }

        Field field = runtimeContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return extractStreamingRuntimeContext(field.get(runtimeContext));
    }

    /**
     * Get the set of bucket IDs that need to be cached by the current lookup join subtask.
     *
     * <p>The Flink Planner will distribute data to lookup join nodes based on buckets. This allows
     * paimon to cache only the necessary buckets for each subtask, improving efficiency.
     *
     * @return the set of bucket IDs to be cached
     */
    protected Set<Integer> getRequireCachedBucketIds() {
        if (strategy == null) {
            return null;
        }
        @Nullable
        Integer indexOfThisSubtask = RuntimeContextUtils.getIndexOfThisSubtask(functionContext);
        @Nullable
        Integer numberOfParallelSubtasks =
                RuntimeContextUtils.getNumberOfParallelSubtasks(functionContext);
        if (indexOfThisSubtask == null) {
            Preconditions.checkState(numberOfParallelSubtasks == null);
            return null;
        } else {
            Preconditions.checkState(numberOfParallelSubtasks != null);
        }
        return strategy.getRequiredCacheBucketIds(indexOfThisSubtask, numberOfParallelSubtasks);
    }

    protected void setCacheRowFilter(@Nullable Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
    }

    private String logRow(List<InternalRow.FieldGetter> fieldGetters, InternalRow row) {
        List<String> rowValues = new ArrayList<>(fieldGetters.size());

        for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
            Object fieldValue = fieldGetter.getFieldOrNull(row);
            String value = fieldValue == null ? "null" : fieldValue.toString();
            rowValues.add(value);
        }
        return rowValues.toString();
    }
}
