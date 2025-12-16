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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory; // 用于创建对lookup缓存文件的读写

    private final int startLevel;

    private IOManager ioManager;

    @Nullable private Cache<String, LookupFile> lookupFileCache; // LocalTableQuery内定义了Caffeine cache实例，配置了基于访问时间的过期策略 (expireAfterAccess) 和最大磁盘占用 (maximumWeight)，这构成了 LRU 的基础

    private final RowType rowType;
    private final RowType partitionType;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.lookupStoreFactory =
                // 根据lookup.local-file-type配置决定本地查找文件的类型（排序型、哈希型）
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(keyType).createSliceComparator());
        startLevel = options.needLookup() ? 1 : 0;
    }

    /**
     * 该方法用于刷新指定分区和桶下的数据文件列表。
     * 如果该分区和桶尚未初始化，则调用 newLookupLevels 方法进行初始化；
     * 否则，复用已有的 LookupLevels 实例，并通过 update 方法更新其内部文件列表，以反映最新的文件变化。
     */
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        LookupLevels<KeyValue> lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);
        if (lookupLevels == null) {
            // Initial phase: ignore beforeFiles as they represent deletions from previous state
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        // 使用传入的 dataFiles (当前有效的数据文件元信息) 和 keyComparator 来初始化 Levels
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache( // 创建文件缓存
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }

        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        levels, // 将创建的 Levels 实例传递给 LookupLevels，其会利用 Levels 提供的分层文件信息来进行高效的键查找，并可能为这些 DataFileMeta 创建本地的 "lookup file" 以进一步加速。当 refreshFiles 被调用且 LookupLevels 实例已存在时，会调用 lookupLevels.getLevels().update(beforeFiles, dataFiles) 来更新 Levels 内部的文件列表
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        new LookupLevels.KeyValueProcessor(readerFactoryBuilder.readValueType()),
                        file -> {
                            RecordReader<KeyValue> reader = factory.createRecordReader(file);
                            if (cacheRowFilter != null) {
                                reader =
                                        reader.filter(
                                                keyValue -> cacheRowFilter.test(keyValue.value()));
                            }
                            return reader;
                        },
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(options),
                        lookupFileCache);

        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /**
     * LookupLevels.lookup(key, startLevel)
     *    |
     *    调用--> 2. LookupUtils.lookup(levels, key, startLevel, llLookupRun, llLookupLevel0)
     *              |
     *              +-- 遍历 levels (从 startLevel 开始):
     *                  |
     *                  +-- IF 当前层级 == 0:
     *                  |   |
     *                  |   调用--> 3. llLookupLevel0(key, level0Files)  (即 LookupLevels.lookupLevel0)
     *                  |             |
     *                  |             调用--> 4. LookupUtils.lookupLevel0(comparator, key, level0Files, llLookupFile)
     *                  |                       |
     *                  |                       +-- 遍历 level0Files 中的每个 DataFileMeta:
     *                  |                           |
     *                  |                           +-- IF key 在文件范围内:
     *                  |                               |
     *                  |                               调用--> 5. llLookupFile(key, dataFileMeta) (即 LookupLevels.lookup(InternalRow, DataFileMeta))
     *                  |                                         |
     *                  |                                         +-- 尝试从 lookupFileCache 获取 LookupFile
     *                  |                                         +-- IF 缓存未命中:
     *                  |                                         |   |
     *                  |                                         |   调用--> 6. LookupLevels.createLookupFile(dataFileMeta)
     *                  |                                         |             (创建本地查找文件, 填充数据)
     *                  |                                         |
     *                  |                                         +-- 从 LookupFile 中获取序列化的 valueBytes
     *                  |                                         +-- 调用 valueProcessor.readFromDisk(...) 转换结果
     *                  |                                         +-- RETURN 结果 (如果找到)
     *                  |
     *                  +-- ELSE (当前层级 > 0):
     *                      |
     *                      调用--> 7. llLookupRun(key, sortedRunForLevel) (即 LookupLevels.lookup(InternalRow, SortedRun))
     *                                |
     *                                调用--> 8. LookupUtils.lookup(comparator, key, sortedRunForLevel, llLookupFile)
     *                                          |
     *                                          +-- 在 sortedRunForLevel 的文件中进行二分查找，找到合适的 DataFileMeta
     *                                          +-- IF 找到文件:
     *                                              |
     *                                              调用--> 5. llLookupFile(key, dataFileMeta) (同上)
     *                                                        |
     *                                                        +-- (与上述步骤 5 逻辑相同)
     *                                                        +-- RETURN 结果 (如果找到)
     */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel); // 核心
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
