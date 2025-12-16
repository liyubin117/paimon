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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/** An {@link Iterator} for a block.
 * 负责在单个内存块内进行迭代和查找
 * */
public abstract class BlockIterator implements Iterator<Map.Entry<MemorySlice, MemorySlice>> {

    protected final MemorySliceInput data;

    private final int recordCount;
    private final Comparator<MemorySlice> comparator;

    private BlockEntry polled;

    /**
     * @param data 块数据
     * @param recordCount 条数
     * @param comparator 比较器
     */
    public BlockIterator(
            MemorySliceInput data, int recordCount, Comparator<MemorySlice> comparator) {
        this.data = data;
        this.recordCount = recordCount;
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        return polled != null || data.isReadable();
    }

    @Override
    public BlockEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (polled != null) {
            BlockEntry result = polled;
            polled = null;
            return result;
        }

        return readEntry();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * 核心方法，使用索引块（通常是一个B+树或者类似的结构，或者简单的排序索引条目）二分查找快速定位可能包含该 key 的数据块。
     * 1.通过 left 和 right 指针维护查找范围。
     * 2.在循环中，计算 mid 位置。
     * 3.调用抽象的 seekTo(int record) 方法，这个方法由子类 (AlignedIterator 或 UnalignedIterator) 实现，用于将 MemorySliceInput data 的读取位置定位到第 record 条记录的开头。
     * 4.调用 readEntry() 方法从当前位置读取一个完整的 (key, value) 条目。
     * 5.使用 comparator 比较读取到的 midEntry.getKey() 和 targetKey。
     * 6.根据比较结果调整 left 或 right 指针，或者如果找到匹配项，则将 midEntry 存入 polled 并返回 true。
     */
    public boolean seekTo(MemorySlice targetKey) {
        int left = 0;
        int right = recordCount - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;

            seekTo(mid);
            BlockEntry midEntry = readEntry();
            int compare = comparator.compare(midEntry.getKey(), targetKey);

            if (compare == 0) {
                polled = midEntry;
                return true;
            } else if (compare > 0) {
                polled = midEntry;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return false;
    }

    public abstract void seekTo(int record);

    private BlockEntry readEntry() {
        requireNonNull(data, "data is null");

        int keyLength;
        keyLength = data.readVarLenInt();
        MemorySlice key = data.readSlice(keyLength);

        int valueLength = data.readVarLenInt();
        MemorySlice value = data.readSlice(valueLength);

        return new BlockEntry(key, value);
    }
}
