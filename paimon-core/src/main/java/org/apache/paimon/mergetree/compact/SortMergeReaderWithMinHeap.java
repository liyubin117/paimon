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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** {@link SortMergeReader} implemented with min-heap.
 * Paimon 合并排序（Merge-Sort）机制中最终执行多路归并（K-way Merge）的核心实现之一。
 * 核心算法：使用最小堆（PriorityQueue）实现经典的 K-way Merge 算法，保证了输出的全局有序性。
 * 批处理感知：通过 readBatch() 和 SortMergeIterator 的分离，以及 nextBatchReaders 列表，优雅地处理了多个输入流的批次边界问题。
 * 状态机迭代：nextImpl 方法中的“补充-合并”循环，以及 polled 列表的使用，构成了一个精巧的状态机。它确保了每次从堆中取出的元素都能在处理后，从其源头获取后继元素并重新放入堆中，从而驱动整个合并过程持续进行。
 * 功能分离：排序逻辑由 Comparator 定义，合并逻辑由 MergeFunction 定义，SortMergeReaderWithMinHeap 本身则专注于高效地驱动整个流程，体现了良好的分层设计。
 * */
public class SortMergeReaderWithMinHeap<T> implements SortMergeReader<T> {

    // 存储了那些当前批次（batch）已经读完，需要等待下一次 readBatch() 调用时再去获取新批次的 RecordReader
    private final List<RecordReader<KeyValue>> nextBatchReaders;
    // 用户主键的比较器，用于比较 KeyValue 的 key 部分，是排序和分组的主要依据
    private final Comparator<InternalRow> userKeyComparator;
    // 合并函数的包装器。当从堆中取出主键相同的一组元素时，这些元素会被喂给 mergeFunctionWrapper 进行处理
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;

    // 最小堆，堆顶永远是所有输入流中最小的那个元素
    private final PriorityQueue<Element> minHeap;
    // 一个临时的 Element 列表。它用于存放上一次从堆中取出、用于合并的元素。这是理解其批处理和迭代逻辑的关键
    private final List<Element> polled;

    public SortMergeReaderWithMinHeap(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers);
        this.userKeyComparator = userKeyComparator;
        this.mergeFunctionWrapper = mergeFunctionWrapper;

        this.minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            // 1. 比较主键
                            int result = userKeyComparator.compare(e1.kv.key(), e2.kv.key());
                            if (result != 0) {
                                return result;
                            }
                            // 2. 如果主键相同，比较用户定义的 sequence 字段
                            if (userDefinedSeqComparator != null) {
                                result =
                                        userDefinedSeqComparator.compare(
                                                e1.kv.value(), e2.kv.value());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            // 3. 如果仍然相同，比较 Paimon 内部的 sequence number
                            return Long.compare(e1.kv.sequenceNumber(), e2.kv.sequenceNumber());
                        });
        this.polled = new ArrayList<>();
    }

    /**
     * 由外部消费者调用，获取一个批次迭代器RecordIterator的入口
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        // 1. 初始化阶段：从每个 reader 中读取第一条记录放入堆中
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            while (true) {
                // 1.1 拿到reader对应的批迭代器
                RecordIterator<KeyValue> iterator = reader.readBatch();
                if (iterator == null) { // 1.2 迭代器已释放，关闭并移除
                    reader.close();
                    break;
                }
                KeyValue kv = iterator.next(); // 1.3 调用iterator.next()拿到一条记录（每个iterator内是有序的，这相当于是第一条记录）
                if (kv == null) { // 1.4 迭代器已读完，释放
                    // empty iterator, clean up and try next batch
                    iterator.releaseBatch();
                } else { // 包装成Element后加到最小堆
                    minHeap.offer(new Element(kv, iterator, reader));
                    break;
                }
            }
        }
        nextBatchReaders.clear(); // 读完这一批reader后清空

        // 2. 完成初始化后，如果最小堆中有数据，就创建一个 SortMergeIterator 实例并返回。这个迭代器将负责从堆中消费数据；若最小堆无数据说明这批reader无数据
        return minHeap.isEmpty() ? null : new SortMergeIterator();
    }

    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
        for (Element element : minHeap) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
        for (Element element : polled) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
    }

    /** The iterator iterates on {@link SortMergeReaderWithMinHeap}. 实际执行迭代和合并逻辑的地方 */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false;

        /**
         * 循环调用 nextImpl()，直到 mergeFunctionWrapper 产生一个结果
         */
        @Override
        public T next() throws IOException {
            while (true) {
                boolean hasMore = nextImpl();
                if (!hasMore) {
                    return null;
                }
                T result = mergeFunctionWrapper.getResult();
                if (result != null) {
                    return result;
                }
            }
        }

        /**
         * 整个算法最核心的部分，它执行一个“取-合并-补”的循环
         * 补充（Refill）：遍历 polled 列表（即上一轮合并的元素），尝试从它们的源 iterator 中获取下一条记录（通过 element.update()）。如果成功，将更新后的 element 重新放入堆中。如果失败（表示该 iterator 的当前批次已读完），则将其 reader 放入 nextBatchReaders，等待外部下一次调用 readBatch()。
         * 检查结束：如果 nextBatchReaders 不为空，说明至少有一个输入流的批次结束了，那么当前 SortMergeIterator 的生命周期也结束了，返回 false。
         * 合并（Merge）：
         *  - 从堆顶 peek() 一个元素，确定当前要合并的 key。
         *  - 进入一个循环，不断从堆中 poll() 出所有与当前 key 相同的元素。
         *  - 将取出的元素交给 mergeFunctionWrapper 处理。
         *  - 同时，将这些取出的 Element 对象存入 polled 列表，以便在下一次调用 nextImpl 时执行第1步的“补充”操作
         */
        private boolean nextImpl() throws IOException {
            Preconditions.checkState(
                    !released, "SortMergeIterator#advanceNext is called after release");
            Preconditions.checkState(
                    nextBatchReaders.isEmpty(),
                    "SortMergeIterator#advanceNext is called even if the last call returns null. "
                            + "This is a bug.");

            // 1. 补充堆：处理上一轮合并过的元素 (polled list)
            for (Element element : polled) {
                if (element.update()) {
                    // 成功从其源 iterator 获取下一条记录，重新放入堆中
                    minHeap.offer(element);
                } else {
                    // 源 iterator 的当前批次已耗尽，将其 reader 放入 nextBatchReaders
                    element.iterator.releaseBatch();
                    nextBatchReaders.add(element.reader);
                }
            }
            polled.clear();

            // 2. 检查批次是否结束
            if (!nextBatchReaders.isEmpty()) {
                return false;
            }

            // 3. 合并阶段：处理下一个主键相同的组
            mergeFunctionWrapper.reset();
            InternalRow key =
                    // 获取堆顶元素的 key
                    Preconditions.checkNotNull(minHeap.peek(), "Min heap is empty. This is a bug.")
                            .kv
                            .key();

            // 循环取出所有 key 相同的元素
            while (!minHeap.isEmpty()) {
                Element element = minHeap.peek();
                if (userKeyComparator.compare(key, element.kv.key()) != 0) {
                    break; // key 不同，分组结束
                }
                minHeap.poll(); // 从堆中取出
                mergeFunctionWrapper.add(element.kv); // 交给 MergeFunction 处理合并
                polled.add(element); // 放入 polled 列表，等待下一轮补充
            }
            return true; // 成功处理了一个分组
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }

    /**
     * 将一条数据 (kv) 与其来源 (iterator 和 reader) 绑定在一起
     */
    private static class Element {
        private KeyValue kv;
        private final RecordIterator<KeyValue> iterator;
        private final RecordReader<KeyValue> reader;

        private Element(
                KeyValue kv, RecordIterator<KeyValue> iterator, RecordReader<KeyValue> reader) {
            this.kv = kv;
            this.iterator = iterator;
            this.reader = reader;
        }

        // 调用iterator.next()获取下一条数据。IMPORTANT: Must not call this for elements still in priority queue!
        private boolean update() throws IOException {
            KeyValue nextKv = iterator.next();
            if (nextKv == null) {
                return false;
            }
            kv = nextKv;
            return true;
        }
    }
}
