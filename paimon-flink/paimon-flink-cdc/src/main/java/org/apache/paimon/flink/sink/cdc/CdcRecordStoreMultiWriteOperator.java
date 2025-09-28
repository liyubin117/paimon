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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWriteStateImpl;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.LOG_CORRUPT_RECORD;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.MAX_RETRY_NUM_TIMES;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME;
import static org.apache.paimon.flink.sink.cdc.CdcRecordStoreWriteOperator.SKIP_CORRUPT_RECORD;
import static org.apache.paimon.flink.sink.cdc.CdcRecordUtils.toGenericRow;

/**
 * A {@link PrepareCommitOperator} to write {@link CdcRecord}. Record schema may change. If current
 * known schema does not fit record schema, this operator will wait for schema changes.
 */
public class CdcRecordStoreMultiWriteOperator
        extends PrepareCommitOperator<CdcMultiplexRecord, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private final StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final CatalogLoader catalogLoader;

    private MemoryPoolFactory memoryPoolFactory;
    private Catalog catalog;
    private Map<Identifier, FileStoreTable> tables; // 表缓存。Key 是表的唯一标识符 Identifier，Value 是该表的 FileStoreTable 对象，其中包含了表的元数据，最重要的就是 Schema
    private StoreSinkWriteState state;
    private Map<Identifier, StoreSinkWrite> writes; // 写入器实例的缓存。Key 同样是 Identifier，Value 是为该表创建的 StoreSinkWrite 实例，负责将数据写入内存缓冲区、刷盘等
    private String commitUser;
    private ExecutorService compactExecutor;

    private CdcRecordStoreMultiWriteOperator(
            StreamOperatorParameters<MultiTableCommittable> parameters,
            CatalogLoader catalogLoader,
            StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider,
            String initialCommitUser,
            Options options) {
        super(parameters, options);
        this.catalogLoader = catalogLoader;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        catalog = catalogLoader.load();

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        // TODO: should use CdcRecordMultiChannelComputer to filter
        state =
                new StoreSinkWriteStateImpl(
                        RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
                        context,
                        (tableName, partition, bucket) -> true);
        tables = new HashMap<>();
        writes = new HashMap<>();
        compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-CdcMultiWrite-Compaction"));
    }

    @Override
    public void processElement(StreamRecord<CdcMultiplexRecord> element) throws Exception {
        CdcMultiplexRecord record = element.getValue();

        String databaseName = record.databaseName();
        String tableName = record.tableName();
        Identifier tableId = Identifier.create(databaseName, tableName);

        FileStoreTable table = getTable(tableId);

        int retryCnt = table.coreOptions().toConfiguration().get(MAX_RETRY_NUM_TIMES);
        boolean skipCorruptRecord = table.coreOptions().toConfiguration().get(SKIP_CORRUPT_RECORD);

        // all table write should share one write buffer so that writers can preempt memory
        // from those of other tables
        if (memoryPoolFactory == null) {
            memoryPoolFactory =
                    new MemoryPoolFactory(
                            memoryPool != null
                                    ? memoryPool
                                    // currently, the options of all tables are the same in CDC
                                    : new HeapMemorySegmentPool(
                                            table.coreOptions().writeBufferSize(),
                                            table.coreOptions().pageSize()));
        }

        StoreSinkWrite write =
                writes.computeIfAbsent(
                        tableId,
                        id ->
                                storeSinkWriteProvider.provide(
                                        table,
                                        commitUser,
                                        state,
                                        getContainingTask().getEnvironment().getIOManager(),
                                        memoryPoolFactory,
                                        getMetricGroup()));

        ((StoreSinkWriteImpl) write).withCompactExecutor(compactExecutor);

        boolean logCorruptRecord = table.coreOptions().toConfiguration().get(LOG_CORRUPT_RECORD);
        Optional<GenericRow> optionalConverted =
                toGenericRow(record.record(), table.schema().fields(), logCorruptRecord);
        // 当schema变更时，由于和旧 Schema 不匹配，转换会失败，optionalConverted 为空
        if (!optionalConverted.isPresent()) {
            FileStoreTable latestTable = table;
            // 进入等待-转换循环，直到转换成功后刷新到文件或者重试次数达到上限
            for (int retry = 0; retry < retryCnt; ++retry) {
                // 关键步骤1：从 Catalog 重新加载表的最新元数据
                latestTable = latestTable.copyWithLatestSchema();

                // 关键步骤2：用新的 Table 对象更新缓存。
                tables.put(tableId, latestTable);

                // 用新的 Schema 再次尝试转换
                optionalConverted =
                        toGenericRow(
                                record.record(), latestTable.schema().fields(), logCorruptRecord);
                if (optionalConverted.isPresent()) {
                    // 转换成功，跳出循环
                    break;
                }
                // 短暂休眠，等待上游的 Schema 变更算子完成对 Catalog 的修改
                Thread.sleep(
                        latestTable
                                .coreOptions()
                                .toConfiguration()
                                .get(RETRY_SLEEP_TIME)
                                .toMillis());
            }
            write.replace(latestTable);
        }

        if (!optionalConverted.isPresent()) {
            if (skipCorruptRecord) {
                LOG.warn(
                        "Skipping corrupt or unparsable record {}",
                        (logCorruptRecord ? record : "<redacted>"));
            } else {
                throw new RuntimeException(
                        "Unable to process element. Possibly a corrupt record: "
                                + (logCorruptRecord ? record : "<redacted>"));
            }
        } else {
            try {
                write.write(optionalConverted.get());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private FileStoreTable getTable(Identifier tableId) throws InterruptedException {
        FileStoreTable table = tables.get(tableId);
        if (table == null) {
            while (true) {
                try {
                    table = (FileStoreTable) catalog.getTable(tableId);
                    tables.put(tableId, table);
                    break;
                } catch (Catalog.TableNotExistException e) {
                    // table not found, waiting until table is created by
                    //     upstream operators
                }
                Thread.sleep(RETRY_SLEEP_TIME.defaultValue().toMillis());
            }
        }

        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Combine mode Sink only supports FIXED bucket mode, but %s is %s",
                            table.name(), table.bucketMode()));
        }
        return table;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        for (StoreSinkWrite write : writes.values()) {
            write.snapshotState();
        }
        state.snapshotState();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (StoreSinkWrite write : writes.values()) {
            write.close();
        }
        if (compactExecutor != null) {
            compactExecutor.shutdownNow();
        }
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            try {
                committables.addAll(
                        write.prepareCommit(waitCompaction, checkpointId).stream()
                                .map(
                                        committable ->
                                                MultiTableCommittable.fromCommittable(
                                                        key, committable))
                                .collect(Collectors.toList()));
            } catch (Exception e) {
                throw new IOException("Failed to prepare commit for table: " + key.toString(), e);
            }
        }
        return committables;
    }

    @VisibleForTesting
    public Map<Identifier, FileStoreTable> tables() {
        return tables;
    }

    @VisibleForTesting
    public Map<Identifier, StoreSinkWrite> writes() {
        return writes;
    }

    @VisibleForTesting
    public String commitUser() {
        return commitUser;
    }

    /** {@link StreamOperatorFactory} of {@link CdcRecordStoreMultiWriteOperator}. */
    public static class Factory
            extends PrepareCommitOperator.Factory<CdcMultiplexRecord, MultiTableCommittable> {
        private final StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider;
        private final String initialCommitUser;
        private final CatalogLoader catalogLoader;

        public Factory(
                CatalogLoader catalogLoader,
                StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider,
                String initialCommitUser,
                Options options) {
            super(options);
            this.catalogLoader = catalogLoader;
            this.storeSinkWriteProvider = storeSinkWriteProvider;
            this.initialCommitUser = initialCommitUser;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<MultiTableCommittable>> T createStreamOperator(
                StreamOperatorParameters<MultiTableCommittable> parameters) {
            return (T)
                    new CdcRecordStoreMultiWriteOperator(
                            parameters,
                            catalogLoader,
                            storeSinkWriteProvider,
                            initialCommitUser,
                            options);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return CdcRecordStoreMultiWriteOperator.class;
        }
    }
}
