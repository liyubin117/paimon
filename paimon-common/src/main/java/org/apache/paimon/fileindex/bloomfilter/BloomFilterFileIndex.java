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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.BloomFilter64;
import org.apache.paimon.utils.BloomFilter64.BitSet;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.util.bloom.HashFunction;

import java.io.IOException;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/**
 * Bloom filter for file index.
 * 是paimon文件级别过滤能力的关键组件之一
 * 提供布隆过滤器的写入逻辑：在数据文件生成时，收集列中的所有值，构建一个布隆过滤器。
 * 提供布隆过滤器的读取和判断逻辑：在查询时，加载布隆过滤器，并用它来快速判断一个查询条件（等值查询）是否绝对不可能在文件中命中。如果布隆过滤器判断不存在，那么就可以安全地跳过整个文件，从而极大地提升查询性能
 *
 * <p>Note: This class use {@link BloomFilter64} as a base filter. Store the num hash function (one
 * integer) and bit set bytes only. Use {@link HashFunction} to hash the objects, which hash bytes
 * type(like varchar, binary, etc.) using xx hash, hash numeric type by specified number hash(see
 * http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).
 */
public class BloomFilterFileIndex implements FileIndexer {

    private static final int DEFAULT_ITEMS = 1_000_000;
    private static final double DEFAULT_FPP = 0.1;

    private static final String ITEMS = "items";
    private static final String FPP = "fpp";

    private final DataType dataType;
    private final int items;
    private final double fpp;

    /**
     * 接收列的 DataType 和用户通过 WITH 子句传入的 Options
     * @param dataType
     * @param options：items (file-index.bloom-filter.items): 预估的列中独立值的数量（NDV），默认为 100 万；
     *               fpp (file-index.bloom-filter.fpp): 期望的假阳性率（False Positive Probability），默认为 0.1。 这两个参数共同决定了布隆过滤器底层位图（BitSet）的大小和哈希函数的数量，是空间占用和准确率之间的权衡
     */
    public BloomFilterFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.items = options.getInteger(ITEMS, DEFAULT_ITEMS);
        this.fpp = options.getDouble(FPP, DEFAULT_FPP);
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, items, fpp);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            byte[] serializedBytes = new byte[length];
            IOUtils.readFully(inputStream, serializedBytes);
            return new Reader(dataType, serializedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构建布隆过滤器并将其序列化
     */
    private static class Writer extends FileIndexWriter {

        private final BloomFilter64 filter; // Paimon 实现的 64 位哈希的布隆过滤器。所有的值都会被添加到这个过滤器中
        private final FastHash hashFunction; // Paimon 为不同的数据类型（数值、字符串等）提供了专门的、高性能的哈希函数，以获得更好的哈希分布。FastHash.getHashFunction(type) 会根据列类型返回最合适的哈希函数

        public Writer(DataType type, int items, double fpp) {
            this.filter = new BloomFilter64(items, fpp);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        /**
         * 每接收一个列值 (key)，就先用 hashFunction 计算出它的 64 位哈希值，然后调用 filter.addHash() 将这个哈希值添加到布隆过滤器中。这个过程会设置底层 BitSet 中的若干个位
         */
        @Override
        public void write(Object key) {
            if (key != null) {
                filter.addHash(hashFunction.hash(key));
            }
        }

        /**
         * 文件写入完成时，这个方法被调用，它定义了 Paimon 布隆过滤器的序列化格式：
         *  前 4 个字节: 以大端序 (Big Endian) 存储哈希函数的数量 (numHashFunctions)。
         *  后续所有字节: 存储布隆过滤器底层的 BitSet 的内容
         */
        @Override
        public byte[] serializedBytes() {
            int numHashFunctions = filter.getNumHashFunctions();
            byte[] serialized = new byte[filter.getBitSet().bitSize() / Byte.SIZE + Integer.BYTES];
            // big endian
            serialized[0] = (byte) ((numHashFunctions >>> 24) & 0xFF);
            serialized[1] = (byte) ((numHashFunctions >>> 16) & 0xFF);
            serialized[2] = (byte) ((numHashFunctions >>> 8) & 0xFF);
            serialized[3] = (byte) (numHashFunctions & 0xFF);
            filter.getBitSet().toByteArray(serialized, 4, serialized.length - 4);
            return serialized;
        }
    }

    /**
     * 反序列化布隆过滤器并提供查询能力
     */
    private static class Reader extends FileIndexReader {

        private final BloomFilter64 filter;
        private final FastHash hashFunction;

        public Reader(DataType type, byte[] serializedBytes) {
            // 从字节数组的前 4 个字节解析出哈希函数的数量。
            // 用剩下的字节构建 BitSet。
            // 使用这两个信息重建一个 BloomFilter64 对象
            // little endian
            int numHashFunctions =
                    ((serializedBytes[0] << 24)
                            + (serializedBytes[1] << 16)
                            + (serializedBytes[2] << 8)
                            + serializedBytes[3]);
            BitSet bitSet = new BitSet(serializedBytes, 4);
            this.filter = new BloomFilter64(numHashFunctions, bitSet);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        /**
         * 查询的核心。当查询引擎传来一个等值过滤条件（如 WHERE col = 'some_value'）时：
         *
         * 1.key 就是 'some_value'。
         * 2.用同样的 hashFunction 计算 key 的哈希值。
         * 3.调用 filter.testHash() 在布隆过滤器中进行判断。
         * 4.结果：
         *  如果 testHash 返回 true（可能存在），则此过滤器无法给出确定性结论，必须继续读取该文件。返回 REMAIN。
         *  如果 testHash 返回 false（绝对不存在），则可以确定该文件中没有任何一行的 col 等于 'some_value'。返回 SKIP，整个文件被跳过。
         */
        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object key) {
            return key == null || filter.testHash(hashFunction.hash(key)) ? REMAIN : SKIP;
        }
    }
}
