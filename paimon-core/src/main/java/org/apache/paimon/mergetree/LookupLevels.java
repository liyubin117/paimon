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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/** Provide lookup by key. */
public class LookupLevels<T> implements Levels.DropFileCallback, Closeable {

    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final RowCompactedSerializer keySerializer;
    private final ValueProcessor<T> valueProcessor;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Function<String, File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;
    private final Function<Long, BloomFilter.Builder> bfGenerator;
    // 由 Caffeine 库实现的缓存，键是远程数据文件的名字，唯一标识，值对应LookupFile对象，即本地副本或索引
    private final Cache<String, LookupFile> lookupFileCache;
    private final Set<String> ownCachedFiles;

    public LookupLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            ValueProcessor<T> valueProcessor,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Function<String, File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Function<Long, BloomFilter.Builder> bfGenerator,
            Cache<String, LookupFile> lookupFileCache) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueProcessor = valueProcessor;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.bfGenerator = bfGenerator;
        this.lookupFileCache = lookupFileCache;
        this.ownCachedFiles = new HashSet<>();
        levels.addDropFileCallback(this);
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFileCache;
    }

    @VisibleForTesting
    Set<String> cachedFiles() {
        return ownCachedFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        lookupFileCache.invalidate(file);
    }

    @Nullable
    public T lookup(InternalRow key, int startLevel) throws IOException {
        return LookupUtils.lookup(levels, key, startLevel, this::lookup, this::lookupLevel0);
    }

    @Nullable
    private T lookupLevel0(InternalRow key, TreeSet<DataFileMeta> level0) throws IOException {
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::lookup);
    }

    @Nullable
    private T lookup(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::lookup);
    }

    /**
     * lookup的关键逻辑：
     * 打开远程文件 -> 逐条读取 -> 写入本地优化文件 -> 关闭本地文件 -> 创建 LookupFile 句柄。这个 LookupFile 对象随后会被放入 lookupFileCache 中供后续查询使用
     *
     * 当 LookupLevels 需要对一个 DataFileMeta 文件进行键查找时（在其 lookup(InternalRow key, DataFileMeta file) 方法中），它会首先尝试从 lookupFileCache 中根据文件名获取 LookupFile。
     * 如果缓存命中，则直接使用返回的 LookupFile 对象进行本地查询，速度很快。
     * 如果缓存未命中，LookupLevels 会调用 createLookupFile 方法创建一个新的 LookupFile 实例（这可能涉及从远程存储下载数据并在本地构建索引），
     * 然后将这个新创建的 LookupFile 放入 lookupFileCache 中，以便后续查询可以复用。
     */
    @Nullable
    private T lookup(InternalRow key, DataFileMeta file) throws IOException {
        LookupFile lookupFile = lookupFileCache.getIfPresent(file.fileName()); // 如果缓存命中则直接使用，避免了网络传输和解析开销

        boolean newCreatedLookupFile = false;
        if (lookupFile == null) {
            lookupFile = createLookupFile(file); // 如果缓存未命中，则构建lookup文件
            newCreatedLookupFile = true;
        }

        byte[] valueBytes;
        try {
            byte[] keyBytes = keySerializer.serializeToBytes(key);
            valueBytes = lookupFile.get(keyBytes); // 从构建出的lookup文件序列化后的字节数组
        } finally {
            if (newCreatedLookupFile) {
                lookupFileCache.put(file.fileName(), lookupFile); // 将获取的结果加到缓存
            }
        }
        if (valueBytes == null) {
            return null;
        }

        return valueProcessor.readFromDisk(
                key, lookupFile.remoteFile().level(), valueBytes, file.fileName()); // 转换结果
    }

    private LookupFile createLookupFile(DataFileMeta file) throws IOException {
        // 读取 DataFileMeta 指向的远程数据文件，根据远程文件名和分区/桶信息生成一个唯一的本地文件名
        File localFile = localFileFactory.apply(file.fileName());
        if (!localFile.createNewFile()) { // 在本地磁盘创建这个空文件
            throw new IOException("Can not create new file: " + localFile);
        }

        // 根据lookup.local-file-type创建LookupStoreWriter，将文件中的所有 Key-Value 对写入一个新的、本地的、为快速查找而优化的文件中
        LookupStoreWriter kvWriter =
                lookupStoreFactory.createWriter(localFile, bfGenerator.apply(file.rowCount()));
        LookupStoreFactory.Context context;
        // 创建RecordReader读取远程DataFileMeta
        try (RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            KeyValue kv;
            if (valueProcessor.withPosition()) {
                FileRecordIterator<KeyValue> batch;
                // 按批循环读取远程文件的每一条kv
                while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                    // 通过LookupStoreWriter把读取到的kv写入本地临时文件
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes =
                                valueProcessor.persistToDisk(kv, batch.returnedPosition());
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            } else {
                RecordReader.RecordIterator<KeyValue> batch;
                while ((batch = reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes = valueProcessor.persistToDisk(kv);
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        } finally {
            // 关闭LookupStoreWriter，确保所有数据的刷盘，完成本地文件的构建，返回包含了写入文件的元数据（索引块、bloomfilter的位置等）的context对象
            context = kvWriter.close();
        }

        // 以远程文件的名字为 Key 存入缓存
        ownCachedFiles.add(file.fileName());
        // 把新建的本地文件、远程文件、新建的LookupStoreReader、清理回调，封装成 LookupFile 对象
        return new LookupFile(
                localFile,
                file,
                lookupStoreFactory.createReader(localFile, context),
                () -> ownCachedFiles.remove(file.fileName()));
    }

    @Override
    public void close() throws IOException {
        Set<String> toClean = new HashSet<>(ownCachedFiles);
        for (String cachedFile : toClean) {
            lookupFileCache.invalidate(cachedFile);
        }
    }

    /** Processor to process value. */
    public interface ValueProcessor<T> {

        boolean withPosition();

        byte[] persistToDisk(KeyValue kv);

        default byte[] persistToDisk(KeyValue kv, long rowPosition) {
            throw new UnsupportedOperationException();
        }

        T readFromDisk(InternalRow key, int level, byte[] valueBytes, String fileName);
    }

    /** A {@link ValueProcessor} to return {@link KeyValue}. */
    public static class KeyValueProcessor implements ValueProcessor<KeyValue> {

        private final RowCompactedSerializer valueSerializer;

        public KeyValueProcessor(RowType valueType) {
            this.valueSerializer = new RowCompactedSerializer(valueType);
        }

        @Override
        public boolean withPosition() {
            return false;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            byte[] vBytes = valueSerializer.serializeToBytes(kv.value());
            byte[] bytes = new byte[vBytes.length + 8 + 1];
            MemorySegment segment = MemorySegment.wrap(bytes);
            segment.put(0, vBytes);
            segment.putLong(bytes.length - 9, kv.sequenceNumber());
            segment.put(bytes.length - 1, kv.valueKind().toByteValue());
            return bytes;
        }

        @Override
        public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
            InternalRow value = valueSerializer.deserialize(bytes);
            long sequenceNumber = MemorySegment.wrap(bytes).getLong(bytes.length - 9);
            RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
            return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
        }
    }

    /** A {@link ValueProcessor} to return {@link Boolean} only. */
    public static class ContainsValueProcessor implements ValueProcessor<Boolean> {

        private static final byte[] EMPTY_BYTES = new byte[0];

        @Override
        public boolean withPosition() {
            return false;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            return EMPTY_BYTES;
        }

        @Override
        public Boolean readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
            return Boolean.TRUE;
        }
    }

    /** A {@link ValueProcessor} to return {@link PositionedKeyValue}. */
    public static class PositionedKeyValueProcessor implements ValueProcessor<PositionedKeyValue> {
        private final boolean persistValue;
        private final RowCompactedSerializer valueSerializer;

        public PositionedKeyValueProcessor(RowType valueType, boolean persistValue) {
            this.persistValue = persistValue;
            this.valueSerializer = persistValue ? new RowCompactedSerializer(valueType) : null;
        }

        @Override
        public boolean withPosition() {
            return true;
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] persistToDisk(KeyValue kv, long rowPosition) {
            if (persistValue) {
                byte[] vBytes = valueSerializer.serializeToBytes(kv.value());
                byte[] bytes = new byte[vBytes.length + 8 + 8 + 1];
                MemorySegment segment = MemorySegment.wrap(bytes);
                segment.put(0, vBytes);
                segment.putLong(bytes.length - 17, rowPosition);
                segment.putLong(bytes.length - 9, kv.sequenceNumber());
                segment.put(bytes.length - 1, kv.valueKind().toByteValue());
                return bytes;
            } else {
                byte[] bytes = new byte[MAX_VAR_LONG_SIZE];
                int len = encodeLong(bytes, rowPosition);
                return Arrays.copyOf(bytes, len);
            }
        }

        @Override
        public PositionedKeyValue readFromDisk(
                InternalRow key, int level, byte[] bytes, String fileName) {
            if (persistValue) {
                InternalRow value = valueSerializer.deserialize(bytes);
                MemorySegment segment = MemorySegment.wrap(bytes);
                long rowPosition = segment.getLong(bytes.length - 17);
                long sequenceNumber = segment.getLong(bytes.length - 9);
                RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
                return new PositionedKeyValue(
                        new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level),
                        fileName,
                        rowPosition);
            } else {
                long rowPosition = decodeLong(bytes, 0);
                return new PositionedKeyValue(null, fileName, rowPosition);
            }
        }
    }

    /** {@link KeyValue} with file name and row position for DeletionVector. */
    public static class PositionedKeyValue {
        private final @Nullable KeyValue keyValue;
        private final String fileName;
        private final long rowPosition;

        public PositionedKeyValue(@Nullable KeyValue keyValue, String fileName, long rowPosition) {
            this.keyValue = keyValue;
            this.fileName = fileName;
            this.rowPosition = rowPosition;
        }

        public String fileName() {
            return fileName;
        }

        public long rowPosition() {
            return rowPosition;
        }

        @Nullable
        public KeyValue keyValue() {
            return keyValue;
        }
    }
}
