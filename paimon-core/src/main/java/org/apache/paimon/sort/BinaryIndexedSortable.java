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

package org.apache.paimon.sort;

import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;

import java.io.IOException;
import java.util.ArrayList;

/**
 * An abstract sortable, provide basic compare and swap. Support writing of index and normalizedKey.
 * 将大量的、耗时的内存拷贝操作，转换成了对轻量级指针和范式化键的快速操作，从而实现了高效的内存排序
 *
 * 数据与索引分离：它将实际的记录数据（recordBuffer）和用于排序的索引（sortIndex）分开存放。
 *  recordBuffer: 这是一个大的、连续的逻辑空间（由多个 MemorySegment 组成），所有的 InternalRow 被序列化后紧凑地存放在这里。一旦写入，这些记录数据就不会再被移动。
 *  sortIndex: 这是另一个独立的内存区域，也由多个 MemorySegment 组成。它不存储完整的记录，而是存储一个个定长的“索引条目”。
 *
 * 索引条目结构：每个索引条目包含两部分：
 *  指针 (Pointer): 一个8字节的 long 类型值，指向 recordBuffer 中对应记录的起始地址（偏移量）。
 *  范式化键 (Normalized Key): 一个定长的字节序列，它是从原始记录的排序列中提取出的一种“指纹”。这个指纹保留了原始键值的排序特性，可以直接按字节进行比较。
 */
public abstract class BinaryIndexedSortable implements IndexedSortable {

    public static final int OFFSET_LEN = 8;

    // --- 核心组件 ---
    // put/compare/swap normalized key
    private final NormalizedKeyComputer normalizedKeyComputer; // 用于生成和比较范式化键，性能优化的关键
    protected final BinaryRowSerializer serializer; // 用于序列化/反序列化记录
    // if normalized key not fully determines, need compare record.
    private final RecordComparator comparator; // 当范式化键无法决定顺序时，用于对反序列化后的记录进行最终比较

    // --- 内存区域 ---
    protected final RandomAccessInputView recordBuffer; // 存储实际记录数据
    private final RandomAccessInputView recordBufferForComparison;
    // segments
    protected MemorySegment currentSortIndexSegment;
    protected final MemorySegmentPool memorySegmentPool; // 内存池，提供 MemorySegment
    protected final ArrayList<MemorySegment> sortIndex; // 存储索引条目

    // --- 索引和范式化键的属性 ---
    // normalized key attributes
    private final int numKeyBytes; // 范式化键的字节数
    protected final int indexEntrySize; // 每个索引条目的大小 (指针 + 范式化键)
    private final int indexEntriesPerSegment; // 每个 MemorySegment 能容纳多少个索引条目
    protected final int lastIndexEntryOffset;
    private final boolean normalizedKeyFullyDetermines;
    private final boolean useNormKeyUninverted;

    // for serialized comparison
    protected final BinaryRowSerializer serializer1;
    private final BinaryRowSerializer serializer2;
    protected final BinaryRow row1;
    private final BinaryRow row2;

    // runtime variables
    protected int currentSortIndexOffset;
    protected int numRecords;

    public BinaryIndexedSortable(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        if (normalizedKeyComputer == null || serializer == null) {
            throw new NullPointerException();
        }
        this.normalizedKeyComputer = normalizedKeyComputer;
        this.serializer = serializer;
        this.comparator = comparator;
        this.memorySegmentPool = memorySegmentPool;
        this.useNormKeyUninverted = !normalizedKeyComputer.invertKey();

        this.numKeyBytes = normalizedKeyComputer.getNumKeyBytes();

        int segmentSize = memorySegmentPool.pageSize();
        this.recordBuffer = new RandomAccessInputView(recordBufferSegments, segmentSize);
        this.recordBufferForComparison =
                new RandomAccessInputView(recordBufferSegments, segmentSize);

        this.normalizedKeyFullyDetermines = normalizedKeyComputer.isKeyFullyDetermines();

        // compute the index entry size and limits
        this.indexEntrySize = numKeyBytes + OFFSET_LEN;
        this.indexEntriesPerSegment = segmentSize / this.indexEntrySize;
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;

        this.serializer1 = serializer.duplicate();
        this.serializer2 = serializer.duplicate();
        this.row1 = this.serializer1.createInstance();
        this.row2 = this.serializer2.createInstance();

        // set to initial state
        this.sortIndex = new ArrayList<>(16);
        this.currentSortIndexSegment = nextMemorySegment();
        sortIndex.add(currentSortIndexSegment);
    }

    protected MemorySegment nextMemorySegment() {
        return this.memorySegmentPool.nextSegment();
    }

    /** check if we need request next index memory. */
    protected boolean checkNextIndexOffset() {
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
            MemorySegment returnSegment = nextMemorySegment();
            if (returnSegment != null) {
                this.currentSortIndexSegment = returnSegment;
                this.sortIndex.add(this.currentSortIndexSegment);
                this.currentSortIndexOffset = 0;
            } else {
                return false;
            }
        }
        return true;
    }

    /** Write of index and normalizedKey.
     * 在子类 BinaryInMemorySortBuffer#write 被调用，每当一条新记录被写入 recordBuffer 后，就会调用此方法来创建对应的索引条目
     * */
    protected void writeIndexAndNormalizedKey(InternalRow record, long currOffset) {
        // 1. 将指向 recordBuffer 的指针 (currOffset) 写入 sortIndex
        this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, currOffset);

        if (this.numKeyBytes != 0) {
            // 2. 计算 record 的 可直接二进制比较的 Normalized Key，并紧接着指针写入 sortIndex
            normalizedKeyComputer.putKey(
                    record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN);
        }

        // 3. 更新索引区的写入位置
        this.currentSortIndexOffset += this.indexEntrySize;
        this.numRecords++;
    }

    @Override
    public int compare(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    // 体现了指针排序的优势：优先进行快速的、基于字节的范式化键比较，只有在必要时才进行昂贵的记录反序列化和比较
    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // 1. 快速路径：比较范式化键
        int val =
                normalizedKeyComputer.compareKey(
                        segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);
        // 2. 如果范式化键不同，或能完全决定顺序，直接返回结果
        if (val != 0 || this.normalizedKeyFullyDetermines) {
            return this.useNormKeyUninverted ? val : -val;
        }
        // 3. 慢速路径：范式化键相同，需要进行深层比较
        final long pointerI = segI.getLong(segmentOffsetI);
        final long pointerJ = segJ.getLong(segmentOffsetJ);
        return compareRecords(pointerI, pointerJ); // 通过指针找到原始记录并比较
    }

    private int compareRecords(long pointer1, long pointer2) {
        this.recordBuffer.setReadPosition(pointer1);
        this.recordBufferForComparison.setReadPosition(pointer2);

        try {
            return this.comparator.compare(
                    serializer1.mapFromPages(row1, recordBuffer),
                    serializer2.mapFromPages(row2, recordBufferForComparison));
        } catch (IOException ioex) {
            throw new RuntimeException("Error comparing two records.", ioex);
        }
    }

    @Override
    public void swap(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    /**
     * 只交换了两个定长的索引条目（指针 + 范式化键），完全没有触及 recordBuffer 中的原始数据，效率极高
     * 被SortBuffer的QuickSort调用
     * @param segmentNumberI index of memory segment containing first record
     * @param segmentOffsetI offset into memory segment containing first record
     * @param segmentNumberJ index of memory segment containing second record
     * @param segmentOffsetJ offset into memory segment containing second record
     */
    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // swap offset
        long index = segI.getLong(segmentOffsetI);
        segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ));
        segJ.putLong(segmentOffsetJ, index);

        // swap key
        normalizedKeyComputer.swapKey(
                segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);
    }

    @Override
    public int size() {
        return this.numRecords;
    }

    @Override
    public int recordSize() {
        return indexEntrySize;
    }

    @Override
    public int recordsPerSegment() {
        return indexEntriesPerSegment;
    }

    /** Spill: Write all records to a {@link AbstractPagedOutputView}.
     * 当排序完成后，这个方法用于按排序后的顺序，将 recordBuffer 中的数据写出到一个输出视图
     * */
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords;
        int currentMemSeg = 0;
        int currentRecord = 0;

        while (currentRecord < numRecords) {
            // 遍历排好序的 sortIndex
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

            // go through all records in the memory segment
            for (int offset = 0;
                    currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                    currentRecord++, offset += this.indexEntrySize) {
                // 从索引中获取指针
                final long pointer = currentIndexSegment.getLong(offset);
                // 用指针定位到 recordBuffer 中的数据
                this.recordBuffer.setReadPosition(pointer);
                // 将数据拷贝到输出
                this.serializer.copyFromPagesToView(this.recordBuffer, output);
            }
        }
    }
}
