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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.FileBasedRandomInputView;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/* This file is based on source code of StorageReader from the PalDB Project (https://github.com/linkedin/PalDB), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Internal read implementation for hash kv store. */
public class HashLookupStoreReader
        implements LookupStoreReader, Iterable<Map.Entry<byte[], byte[]>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HashLookupStoreReader.class.getName());

    // Key count for each key length，keyCounts[L] 表示键长度为 L 的数量
    private final int[] keyCounts;
    // Slot size for each key length，slotSizes[L] 表示键长度为 L 的哈希槽（slot）的大小。每个槽通常包含键本身和指向实际值数据的偏移量
    private final int[] slotSizes;
    // Number of slots for each key length，slots[L] 表示键长度为 L 的哈希槽的数量
    private final int[] slots;
    // Offset of the index for different key length，indexOffsets[L]表示键长度为L的索引数据在文件中的起始偏移量
    private final int[] indexOffsets;
    // Offset of the data for different key length，dataOffsets[L]表示键长度为L的实际值数据在文件中的起始偏移量
    private final long[] dataOffsets;
    // File input view，处理磁盘I/O和缓存的核心
    private FileBasedRandomInputView inputView;
    // Buffers，大小为所有 slotSizes 中的最大值。这个缓冲区用于临时存储从磁盘读取的单个哈希槽的数据
    private final byte[] slotBuffer;

    @Nullable private FileBasedBloomFilter bloomFilter;

    /**
     * 打开 localFile，根据 HashContext (从 Writer 的 close() 获取) 重建或加载哈希表的元数据结构（比如桶数组的起始位置和大小）。加载 Bloom Filter (如果存在)
     */
    HashLookupStoreReader(
            File file,
            HashContext context,
            CacheManager cacheManager,
            int cachePageSize,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        // File path
        if (!file.exists()) {
            throw new FileNotFoundException("File " + file.getAbsolutePath() + " not found");
        }

        keyCounts = context.keyCounts;
        slots = context.slots;
        slotSizes = context.slotSizes;
        int maxSlotSize = 0;
        for (int slotSize : slotSizes) {
            maxSlotSize = Math.max(maxSlotSize, slotSize);
        }
        slotBuffer = new byte[maxSlotSize];
        indexOffsets = context.indexOffsets;
        dataOffsets = context.dataOffsets;

        LOG.info("Opening file {}", file.getName());

        PageFileInput fileInput = // 以页（Page）为单位从文件中读取数据，并可以处理解压缩（如果数据是压缩的）
                PageFileInput.create(
                        file,
                        cachePageSize,
                        compressionFactory,
                        context.uncompressBytes,
                        context.compressPages);
        /**
         * 封装了 PageFileInput，并与 CacheManager 协作。当需要从文件的某个位置读取数据时：
         *  首先检查 CacheManager 是否已经缓存了包含该位置的数据页。
         *  缓存命中：如果命中，直接从缓存中获取数据。
         *  缓存未命中：如果未命中，FileBasedRandomInputView 会通过 PageFileInput 从磁盘读取相应的数据页，将其加载到 CacheManager 中，然后再提供给上层调用
         */
        inputView = new FileBasedRandomInputView(fileInput, cacheManager);

        if (context.bloomFilterEnabled) {
            bloomFilter =
                    // FileBasedBloomFilter 本身也使用 PageFileInput 和 CacheManager 来按需加载和缓存布隆过滤器的数据。布隆过滤器的数据通常存储在文件的开头部分。
                    new FileBasedBloomFilter(
                            fileInput,
                            cacheManager,
                            context.bloomFilterExpectedEntries,
                            0,
                            context.bloomFilterBytes);
        }
    }

    /**
     * 计算哈希: 计算输入 key 的哈希值。
     * Bloom Filter 检查: (如果存在) 首先用 Bloom Filter 测试。
     * 定位桶: 根据哈希值找到对应的桶。
     * 桶内查找: 读取该桶的数据。由于可能存在哈希冲突，需要在桶内（例如，一个链表或连续存储的条目）遍历并比较实际的键，直到找到匹配的 key 或确认不存在。
     * 如果找到匹配的 key，返回其 value。
     */
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        int keyLength = key.length;
        // key.length过滤
        if (keyLength >= slots.length || keyCounts[keyLength] == 0) {
            return null;
        }

        int hashcode = MurmurHashUtils.hashBytes(key); // 计算键的哈希值
        // lookup 操作开始时，会先用布隆过滤器测试键的哈希值。如果布隆过滤器指示键可能不存在，则直接返回 null，避免了后续更昂贵的哈希表查找和磁盘读取
        if (bloomFilter != null && !bloomFilter.testHash(hashcode)) {
            return null;
        }

        // 根据 key.length 从内存中的元数据数组（slots, slotSizes, indexOffsets, dataOffsets）获取该键长度对应的哈希表参数
        long hashPositive = hashcode & 0x7fffffff;
        int numSlots = slots[keyLength];
        int slotSize = slotSizes[keyLength];
        int indexOffset = indexOffsets[keyLength];
        long dataOffset = dataOffsets[keyLength];
        // 通过线性探测法在哈希表中查找对应的槽
        for (int probe = 0; probe < numSlots; probe++) {
            long slot = (hashPositive + probe) % numSlots;
            inputView.setReadPosition(indexOffset + slot * slotSize);  // // 计算并定位到目标槽在文件中的绝对偏移量：indexOffset + slot * slotSize
            inputView.readFully(slotBuffer, 0, slotSize); // 将该槽的数据从磁盘（或缓存）读入内存中的 slotBuffer

            long offset = VarLengthIntUtils.decodeLong(slotBuffer, keyLength); // 从 slotBuffer 中解析出存储的键（或键的前缀）和指向实际值数据的偏移量 (offset)
            if (offset == 0) {
                return null;
            }
            if (isKey(slotBuffer, key)) { // 将 slotBuffer 中存储的键与给定的 key 进行比较，若匹配
                // 计算实际值数据在文件中的绝对偏移量dataOffset + offset，调用getValue获取值
                return getValue(dataOffset + offset);
            }
        }
        return null;
    }

    private boolean isKey(byte[] slotBuffer, byte[] key) {
        for (int i = 0; i < key.length; i++) {
            if (slotBuffer[i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] getValue(long offset) throws IOException {
        inputView.setReadPosition(offset); // 定位到值数据在文件中的起始位置

        // Get size of data，读取值的长度（通常是变长编码的整数）。这会触发一次小的读取操作（可能从缓存或磁盘）
        int size = VarLengthIntUtils.decodeInt(inputView);

        // Create output bytes
        byte[] res = new byte[size]; // 根据读取到的长度创建一个新的字节数组 res
        inputView.readFully(res); // 将完整的实际值数据读入 res 数组
        return res;
    }

    @Override
    public void close() throws IOException {
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        inputView.close();
        inputView = null;
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        return new StorageIterator(true);
    }

    public Iterator<Map.Entry<byte[], byte[]>> keys() {
        return new StorageIterator(false);
    }

    /**
     * 1.迭代器用于遍历存储中的所有键值对（或仅键）。
     * 2.它会按键的长度顺序遍历。对于每种键长度：
     *  它会顺序扫描该键长度对应的整个索引区域（哈希槽区域）。
     *  inputView.setReadPosition(currentIndexOffset) 定位到当前要读取的索引槽。
     *  inputView.readFully(currentSlotBuffer) 将索引槽数据读入内存中的 currentSlotBuffer。
     *  如果槽非空，则从中提取键。
     *  如果需要值 (withValue == true)，则像 lookup 方法一样，使用从槽中获取的偏移量通过 getValue() 方法读取实际的值数据。
     * 3.迭代器也会充分利用 FileBasedRandomInputView 和 CacheManager。由于是顺序扫描，缓存的预读（如果 PageFileInput 或 CacheManager 支持）可能会有较好的效果。
     */
    private class StorageIterator implements Iterator<Map.Entry<byte[], byte[]>> {

        private final FastEntry entry = new FastEntry();
        private final boolean withValue;
        private int currentKeyLength = 0;
        private byte[] currentSlotBuffer;
        private long keyIndex;
        private long keyLimit;
        private long currentDataOffset;
        private int currentIndexOffset;

        public StorageIterator(boolean value) {
            withValue = value;
            nextKeyLength();
        }

        private void nextKeyLength() {
            for (int i = currentKeyLength + 1; i < keyCounts.length; i++) {
                long c = keyCounts[i];
                if (c > 0) {
                    currentKeyLength = i;
                    keyLimit += c;
                    currentSlotBuffer = new byte[slotSizes[i]];
                    currentIndexOffset = indexOffsets[i];
                    currentDataOffset = dataOffsets[i];
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return keyIndex < keyLimit;
        }

        @Override
        public FastEntry next() {
            try {
                inputView.setReadPosition(currentIndexOffset);

                long offset = 0;
                while (offset == 0) {
                    inputView.readFully(currentSlotBuffer);
                    offset = VarLengthIntUtils.decodeLong(currentSlotBuffer, currentKeyLength);
                    currentIndexOffset += currentSlotBuffer.length;
                }

                byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);
                byte[] value = null;

                if (withValue) {
                    long valueOffset = currentDataOffset + offset;
                    value = getValue(valueOffset);
                }

                entry.set(key, value);

                if (++keyIndex == keyLimit) {
                    nextKeyLength();
                }
                return entry;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private class FastEntry implements Map.Entry<byte[], byte[]> {

            private byte[] key;
            private byte[] val;

            protected void set(byte[] k, byte[] v) {
                this.key = k;
                this.val = v;
            }

            @Override
            public byte[] getKey() {
                return key;
            }

            @Override
            public byte[] getValue() {
                return val;
            }

            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException("Not supported.");
            }
        }
    }
}
