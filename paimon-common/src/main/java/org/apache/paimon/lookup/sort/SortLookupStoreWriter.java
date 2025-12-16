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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.paimon.lookup.sort.BlockHandle.writeBlockHandle;
import static org.apache.paimon.lookup.sort.SortLookupStoreUtils.crc32c;
import static org.apache.paimon.memory.MemorySegmentUtils.allocateReuseBytes;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/** A {@link LookupStoreWriter} for sorting.
 * 用于创建基于排序的键值存储（Lookup Store）文件的核心组件。
 * 负责将有序的键值对高效地写入磁盘，并构建相应的索引和元数据，以便 SortLookupStoreReader 能够快速地进行查找。
 * 依赖于调用者已经把键值对排好序然后调用put方法，本身不执行排序
 * */
public class SortLookupStoreWriter implements LookupStoreWriter {

    private static final Logger LOG =
            LoggerFactory.getLogger(SortLookupStoreWriter.class.getName());

    public static final int MAGIC_NUMBER = 1481571681;

    private final BufferedOutputStream fileOutputStream;
    private final int blockSize;
    private final BlockWriter dataBlockWriter; // 数据块写入器
    private final BlockWriter indexBlockWriter; // 索引块写入器
    @Nullable private final BloomFilter.Builder bloomFilter;
    private final BlockCompressionType compressionType;
    @Nullable private final BlockCompressor blockCompressor;

    private byte[] lastKey;
    private long position;

    private long recordCount;
    private long totalUncompressedSize;
    private long totalCompressedSize;

    SortLookupStoreWriter(
            File file,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        this.fileOutputStream = new BufferedOutputStream(Files.newOutputStream(file.toPath()));
        this.blockSize = blockSize;
        this.dataBlockWriter = new BlockWriter((int) (blockSize * 1.1));
        int expectedNumberOfBlocks = 1024;
        this.indexBlockWriter =
                new BlockWriter(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks);
        this.bloomFilter = bloomFilter;
        if (compressionFactory == null) {
            this.compressionType = BlockCompressionType.NONE;
            this.blockCompressor = null;
        } else {
            this.compressionType = compressionFactory.getCompressionType();
            this.blockCompressor = compressionFactory.getCompressor();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        dataBlockWriter.add(key, value); // 数据被组织成数据块（Data Block），每个数据块都包含若干有序的键值对。当内存中的 dataBlockWriter 累积的数据达到预设的 blockSize 时，或者在最后关闭写入器时，数据块会被刷到磁盘
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key)); // 键的哈希值会被添加到布隆过滤器，帮助SortLookupStoreReader快速排除不存在的键
        }

        lastKey = key; // 记录该数据块的最大键

        if (dataBlockWriter.memory() > blockSize) {
            flush();
        }

        recordCount++;
    }

    private void flush() throws IOException {
        if (dataBlockWriter.size() == 0) {
            return;
        }

        BlockHandle blockHandle = writeBlock(dataBlockWriter);
        MemorySlice handleEncoding = writeBlockHandle(blockHandle);
        // 作为索引条目添加到内存中的 indexBlockWriter
        indexBlockWriter.add(lastKey, handleEncoding.copyBytes());
    }

    private BlockHandle writeBlock(BlockWriter blockWriter) throws IOException {
        // close the block
        MemorySlice block = blockWriter.finish();

        totalUncompressedSize += block.length();

        // attempt to compress the block
        BlockCompressionType blockCompressionType = BlockCompressionType.NONE;
        if (blockCompressor != null) {
            int maxCompressedSize = blockCompressor.getMaxCompressedSize(block.length());
            byte[] compressed = allocateReuseBytes(maxCompressedSize + 5);
            int offset = encodeInt(compressed, 0, block.length());
            int compressedSize =
                    offset
                            + blockCompressor.compress(
                                    block.getHeapMemory(),
                                    block.offset(),
                                    block.length(),
                                    compressed,
                                    offset);

            // Don't use the compressed data if compressed less than 12.5%,
            // 只有当压缩效果达到一定阈值（压缩后大小小于原始大小的 7/8）时，才会实际使用压缩后的数据
            if (compressedSize < block.length() - (block.length() / 8)) {
                block = new MemorySlice(MemorySegment.wrap(compressed), 0, compressedSize);
                blockCompressionType = this.compressionType;
            }
        }

        totalCompressedSize += block.length();

        // create block trailer
        // 每个数据块和索引块的尾部都包含CRC32C校验和，用于保证数据完整性
        BlockTrailer blockTrailer =
                new BlockTrailer(blockCompressionType, crc32c(block, blockCompressionType));
        MemorySlice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block，记录该数据块在文件中的位置和大小，包装成BlockHandle
        BlockHandle blockHandle = new BlockHandle(position, block.length());

        // write data
        writeSlice(block);

        // write trailer: 5 bytes
        writeSlice(trailer);

        // clean up state
        blockWriter.reset();

        return blockHandle;
    }

    @Override
    public LookupStoreFactory.Context close() throws IOException {
        // flush current data block
        flush();

        LOG.info("Number of record: {}", recordCount);

        // write bloom filter
        @Nullable BloomFilterHandle bloomFilterHandle = null;
        if (bloomFilter != null) {
            MemorySegment buffer = bloomFilter.getBuffer();
            bloomFilterHandle =
                    new BloomFilterHandle(position, buffer.size(), bloomFilter.expectedEntries());
            writeSlice(MemorySlice.wrap(buffer));
            LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
        }

        // write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockWriter);

        // write footer
        Footer footer = new Footer(bloomFilterHandle, indexBlockHandle);
        MemorySlice footerEncoding = Footer.writeFooter(footer);
        writeSlice(footerEncoding);

        // close file
        fileOutputStream.close();

        LOG.info("totalUncompressedSize: {}", MemorySize.ofBytes(totalUncompressedSize));
        LOG.info("totalCompressedSize: {}", MemorySize.ofBytes(totalCompressedSize));
        return new SortContext(position);
    }

    private void writeSlice(MemorySlice slice) throws IOException {
        fileOutputStream.write(slice.getHeapMemory(), slice.offset(), slice.length());
        position += slice.length();
    }
}
