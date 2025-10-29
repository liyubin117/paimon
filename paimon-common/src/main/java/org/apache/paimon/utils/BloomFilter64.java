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

package org.apache.paimon.utils;

/** Bloom filter 64 handle 64 bits hash.
 * 布隆过滤器功能的核心底层实现，负责处理具体的数学和位运算逻辑
 * 提供一个高效、低内存占用的布隆过滤器实现。它的名字 64 强调了它处理的是 64位的哈希值（long）。这与 Paimon 中另一个 BloomFilter 类（处理32位哈希值）形成了对比。使用 64 位哈希可以进一步降低哈希冲突的概率
 * BloomFilterFileIndex 是面向 Paimon 索引框架的“门面”，而 BloomFilter64 则是真正干活的“引擎”
 *
 * 主要职责包括：
 * 1.根据预估元素数量和期望假阳性率，计算并初始化布隆过滤器所需的最佳参数（位图大小、哈希函数个数）。
 * 2.提供 addHash 方法，将一个 64 位哈希值添加到过滤器中。
 * 3.提供 testHash 方法，判断一个 64 位哈希值是否可能存在于过滤器中。
 * 4.与内部的 BitSet 类协作，完成底层的位操作
 * */
public final class BloomFilter64 {

    private final BitSet bitSet;
    private final int numBits; // 位图（BitSet）的总位数
    private final int numHashFunctions; // 哈希函数的个数

    /**
     * 创建新的布隆过滤器
     * @param items 预估要插入的元素数量
     * @param fpp 误判率，即假阳性概率False Positive Probability
     */
    public BloomFilter64(long items, double fpp) {
        // 1. 计算最优的位数 (m)
        int nb = (int) (-items * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        // 2. 将位数向上对齐到字节的整数倍
        this.numBits = nb + (Byte.SIZE - (nb % Byte.SIZE));
        // 3. 计算最优的哈希函数个数 (k)
        this.numHashFunctions =
                Math.max(1, (int) Math.round((double) numBits / items * Math.log(2)));
        // 4. 初始化底层的 BitSet
        this.bitSet = new BitSet(new byte[numBits / Byte.SIZE], 0);
    }

    /**
     * 从已有数据加载布隆过滤器
     * @param numHashFunctions 哈希函数个数
     * @param bitSet 位图
     */
    public BloomFilter64(int numHashFunctions, BitSet bitSet) {
        this.numHashFunctions = numHashFunctions;
        this.numBits = bitSet.bitSize();
        this.bitSet = bitSet;
    }

    // "Kirsch-Mitzenmacher" 优化 的技巧来模拟多个哈希函数，将计算出的所有 pos 对应的位设置为 1
    public void addHash(long hash64) {
        // 1. 将 64 位哈希拆分为两个 32 位哈希
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        // 2. 循环 k 次，模拟 k 个哈希函数
        for (int i = 1; i <= numHashFunctions; i++) {
            // 3. 生成组合哈希：g_i(x) = h1(x) + i * h2(x)
            int combinedHash = hash1 + (i * hash2);
            // 4. 确保哈希值为正数
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            // 5. 计算在位图中的位置并设置该位
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    // 检查计算出的所有 pos 对应的位是否都为 1。只要有一个位是 0，就可以立即断定元素不存在，并返回 false
    public boolean testHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            // 只要有一个位没有被设置，就说明元素肯定不存在
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        // 所有位都被设置了，说明元素可能存在
        return true;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    /** Bit set used for bloom filter 64. */
    public static class BitSet {

        private static final byte MAST = 0x07; // 等价于二进制 00000111

        private final byte[] data;
        private final int offset;

        public BitSet(byte[] data, int offset) {
            assert data.length > 0 : "data length is zero!";
            assert offset >= 0 : "offset is negative!";
            this.data = data;
            this.offset = offset;
        }

        public void set(int index) {
            // 找到目标位所在的字节: index / 8  (等价于 index >>> 3，无符号右移3位)
            // 找到在字节中的具体位:   index % 8  (等价于 index & 7, 即 index & MAST，获取index的低3位作为位偏移量)
            // 设置位：将目标位设置为1，使用按位或运算 |= 实现
            data[(index >>> 3) + offset] |= (byte) ((byte) 1 << (index & MAST));
        }

        public boolean get(int index) {
            // 使用按位与 & 操作来检查目标位是否为1
            return (data[(index >>> 3) + offset] & ((byte) 1 << (index & MAST))) != 0;
        }

        public int bitSize() {
            return (data.length - offset) * Byte.SIZE;
        }

        public void toByteArray(byte[] bytes, int offset, int length) {
            if (length >= 0) {
                System.arraycopy(data, this.offset, bytes, offset, length);
            }
        }
    }
}
