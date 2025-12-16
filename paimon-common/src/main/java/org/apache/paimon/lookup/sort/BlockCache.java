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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.CacheManager.SegmentContainer;
import org.apache.paimon.memory.MemorySegment;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Cache for block reading.
 * 实现按需加载、缓存的关键组件
 * */
public class BlockCache implements Closeable {

    private final RandomAccessFile file;
    private final FileChannel channel;
    private final CacheManager cacheManager;
    private final Map<CacheKey, SegmentContainer> blocks; // 跟踪通过此 BlockCache 实例访问过的块

    public BlockCache(RandomAccessFile file, CacheManager cacheManager) {
        this.file = file;
        this.channel = this.file.getChannel();
        this.cacheManager = cacheManager;
        this.blocks = new HashMap<>();
    }

    private byte[] readFrom(long offset, int length) throws IOException {
        byte[] buffer = new byte[length];
        int read = channel.read(ByteBuffer.wrap(buffer), offset);

        if (read != length) {
            throw new IOException("Could not read all the data");
        }
        return buffer;
    }

    /**
     * 核心方法，当需要读取文件的一个特定块（由 position 和 length 定义）时，会调用此方法
     */
    public MemorySegment getBlock(
            long position, int length, Function<byte[], byte[]> decompressFunc, boolean isIndex) {
        // 1.构造一个CacheKey，包含了文件、位置、长度以及一个 isIndex 标志（用于区分索引块和数据块，CacheManager 可能会根据此信息采用不同的缓存策略）
        CacheKey cacheKey = CacheKey.forPosition(file, position, length, isIndex);
        // 2.优先从blocks中获取缓存
        SegmentContainer container = blocks.get(cacheKey);
        // 3.若cache miss
        if (container == null || container.getAccessCount() == CacheManager.REFRESH_COUNT) {
            MemorySegment segment =
                    // 尝试从 CacheManager 获取page
                    cacheManager.getPage(
                            cacheKey,
                            key -> {
                                // 通过readFrom调用FileChannel#read从磁盘读原始字节
                                byte[] bytes = readFrom(position, length);
                                // 对读取到的原始字节应用 decompressFunc。这个函数负责解压缩（如果数据块是压缩的）或者直接返回原始字节（如果未压缩或者对于元数据块）
                                return decompressFunc.apply(bytes);
                            },
                            blocks::remove);
            container = new SegmentContainer(segment);
            blocks.put(cacheKey, container); // 加到缓存
        }
        return container.access();
    }

    @Override
    public void close() throws IOException {
        Set<CacheKey> sets = new HashSet<>(blocks.keySet());
        for (CacheKey key : sets) {
            cacheManager.invalidPage(key);
        }
    }
}
