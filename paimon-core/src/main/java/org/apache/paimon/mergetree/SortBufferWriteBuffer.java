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
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/** A {@link WriteBuffer} which stores records in {@link BinaryInMemorySortBuffer}.
 * 实现了 WriteBuffer 接口，是 LSM-Tree (Log-Structured Merge-Tree) 架构中内存缓冲区的核心实现。
 * 主要职责是在数据写入磁盘前，在内存中对写入的记录进行缓冲、排序和预合并
 *
 * 内存计算：在内存中完成排序和合并，效率远高于磁盘。
 * 外部排序：通过溢写机制，优雅地解决了大数据量写入时的内存瓶颈问题。
 * 代码生成：在运行时动态生成比较逻辑的字节码，避免了解释执行的开销，是性能优化的利器。
 * 预合并/预聚合：在数据刷盘前进行合并，有效减少了写放大（Write Amplification）问题，提升了整体写入性能和磁盘IO效率
 *
 * 流程：
 * 1.接收数据：接收上游算子发来的 KeyValue 数据（包含主键、序列号、行类型和行数据）。
 * 2.内存排序：将接收到的数据在内存中按照指定的排序规则进行排序。
 * 3.溢写 (Spillable)：当内存使用达到阈值时，能将排序好的数据溢写到磁盘临时文件中，以防止内存溢出（OOM），并支持处理远超内存大小的数据量。
 * 4.提供有序迭代器：当内存缓冲区需要被刷盘（Flush）时，它能提供一个全局有序的迭代器，该迭代器会合并内存中和所有已溢写到磁盘的临时文件中的数据。
 * 5.预合并 (Pre-aggregation/Merging)：在数据刷盘前，通过 forEach 方法对主键相同的数据进行合并（例如，对于主键表，只保留序列号最大的那条记录），从而减少写入磁盘的数据量。
 * */
public class SortBufferWriteBuffer implements WriteBuffer {

    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueSerializer serializer;
    private final SortBuffer buffer;

    public SortBufferWriteBuffer(
            RowType keyType,
            RowType valueType,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MemorySegmentPool memoryPool,
            boolean spillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions compression,
            IOManager ioManager) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializer = new KeyValueSerializer(keyType, valueType);

        // 1. 确定排序字段的索引数组，排序优先级为：主键字段 > 用户定义的序列号字段 (可选) > 内部序列号 (sequenceNumber)。这样可以确保相同主键的数据被排在一起，并且可以根据序列号确定其先后顺序
        IntStream sortFields = IntStream.range(0, keyType.getFieldCount()); // 主键
        // user define sequence fields
        if (userDefinedSeqComparator != null) {
            IntStream udsFields = // sequence.field
                    IntStream.of(userDefinedSeqComparator.compareFields())
                            .map(operand -> operand + keyType.getFieldCount() + 2);
            sortFields = IntStream.concat(sortFields, udsFields);
        }
        // sequence field
        sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));
        int[] sortFieldArray = sortFields.toArray();

        // 2. 构造完整的行类型
        List<DataType> fieldTypes = new ArrayList<>(keyType.getFieldTypes());
        fieldTypes.add(new BigIntType(false));
        fieldTypes.add(new TinyIntType(false));
        fieldTypes.addAll(valueType.getFieldTypes());

        // 3. 通过codegen创建比较器和正规化键计算器
        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(fieldTypes, sortFieldArray);
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(fieldTypes, sortFieldArray, true);

        if (memoryPool.freePages() < 3) {
            throw new IllegalArgumentException(
                    "Write buffer requires a minimum of 3 page memory, please increase write buffer memory size.");
        }
        InternalRowSerializer serializer =
                InternalSerializers.create(KeyValue.schema(keyType, valueType));
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer, serializer, keyComparator, memoryPool);
        // 4. 根据是否支持溢写，选择不同的 SortBuffer 实现
        this.buffer =
                ioManager != null && spillable
                        ? new BinaryExternalSortBuffer(
                                new BinaryRowSerializer(serializer.getArity()),
                                keyComparator,
                                memoryPool.pageSize(),
                                inMemorySortBuffer,
                                ioManager,
                                sortMaxFan,
                                compression,
                                maxDiskSize)
                        : inMemorySortBuffer;
    }

    /**
     * 数据写入的入口。它将传入的 key, value 等信息通过 KeyValueSerializer 序列化成一个 InternalRow 对象，然后直接调用底层 buffer（BinaryInMemorySortBuffer 或 BinaryExternalSortBuffer）的 write 方法。write 方法会将这条记录添加到排序缓冲区中
     */
    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException {
        return buffer.write(serializer.toRow(key, sequenceNumber, valueKind, value));
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public long memoryOccupancy() {
        return buffer.getOccupancy();
    }

    @Override
    public boolean flushMemory() throws IOException {
        return buffer.flushMemory();
    }

    // 数据从缓冲区读出并处理的核心方法，在缓冲区刷盘（Flush）时被调用
    @Override
    public void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException {
        // TODO do not use iterator
        MergeIterator mergeIterator = // 遍历有序的数据流，将主键相同的连续记录分组，并应用 mergeFunction 进行合并。例如，对于主键表，mergeFunction 的逻辑通常是保留序列号最大的一条记录
                new MergeIterator(
                        rawConsumer, buffer.sortedIterator(), keyComparator, mergeFunction);
        while (mergeIterator.hasNext()) { // 每产出一个合并后的结果，就通过 mergedConsumer 消费掉，通常是写入到下一阶段的 Writer 中（例如，SortedRunDataFile.Writer）
            mergedConsumer.accept(mergeIterator.next());
        }
    }

    @Override
    public void clear() {
        buffer.clear();
    }

    @VisibleForTesting
    SortBuffer buffer() {
        return buffer;
    }

    /**
     * 主要职责是在数据从内存缓冲区刷写（flush）时，对排序后的数据进行归并操作。简单来说，它会遍历排序好的记录，将主键（key）相同的记录合并成一条，然后将合并后的结果交给调用方
     */
    private class MergeIterator {
        @Nullable private final KvConsumer rawConsumer;
        private final MutableObjectIterator<BinaryRow> kvIter; // 从 SortBuffer 获取的、已经排好序的记录迭代器
        private final Comparator<InternalRow> keyComparator; // 键比较器。用来比较两条记录的 key 是否相等，这是将记录分组的依据
        private final ReducerMergeFunctionWrapper mergeFunctionWrapper; // 合并逻辑封装。它包装了真正的合并函数（MergeFunction），负责将多条 key 相同的记录合并成一条。例如，对于 value-kind 类型的表，它会保留最新的记录；对于聚合类型的表，它会执行聚合计算
        private final boolean requireCopy;

        // 状态指针。这是性能优化的关键。为了避免在迭代中频繁创建新对象导致GC压力，这里复用了 BinaryRow 和 KeyValueSerializer 对象。previousRow 始终指向上一个处理的记录，currentRow 指向当前从 kvIter 读取的记录
        // previously read kv
        private KeyValueSerializer previous;
        private BinaryRow previousRow;
        // reads the next kv
        private KeyValueSerializer current;
        private BinaryRow currentRow;

        private KeyValue result; // 合并结果。存放 mergeFunctionWrapper 对一组记录完成合并后生成的最终 KeyValue
        private boolean advanced; // 迭代状态标志。这是一个典型的懒加载（lazy-loading）迭代器模式的标志位，确保 advanceIfNeeded（实际的迭代和合并逻辑）在每次调用 next() 时只执行一次

        private MergeIterator(
                @Nullable KvConsumer rawConsumer,
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunction<KeyValue> mergeFunction)
                throws IOException {
            this.rawConsumer = rawConsumer;
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionWrapper = new ReducerMergeFunctionWrapper(mergeFunction);
            this.requireCopy = mergeFunction.requireCopy();

            int totalFieldCount = keyType.getFieldCount() + 2 + valueType.getFieldCount();
            this.previous = new KeyValueSerializer(keyType, valueType);
            this.previousRow = new BinaryRow(totalFieldCount);
            this.current = new KeyValueSerializer(keyType, valueType);
            this.currentRow = new BinaryRow(totalFieldCount);
            readOnce(); // 1.启动：构造函数会调用 readOnce() 预读第一条记录到 currentRow 中
            this.advanced = false;
        }

        public boolean hasNext() throws IOException {
            advanceIfNeeded();
            return previousRow != null;
        }

        public KeyValue next() throws IOException {
            advanceIfNeeded();
            if (previousRow == null) {
                return null;
            }
            advanced = false; // 设置false，触发advanceIfNeeded()不直接return
            return result;
        }

        /**
         * 核心逻辑，实现了“按键分组并合并”的算法
         * advanceIfNeeded不断地从有序迭代器中读取记录，如果当前记录的主键与上一条记录相同，就将它们都喂给 mergeFunctionWrapper。当遇到一个不同主键的记录时，就意味着上一组相同主key的记录已经全部集齐，此时便从 mergeFunctionWrapper 中获取最终的合并结果。
         */
        private void advanceIfNeeded() throws IOException {
            if (advanced) {
                return;
            }
            advanced = true;

            do {
                // 1. 开始新分组
                swapSerializers(); // 将 currentRow 的内容交换给 previousRow，这标志着一个新 key 分组的处理开始了
                if (previousRow == null) {
                    return;
                }
                mergeFunctionWrapper.reset(); // 清空上一组的合并状态
                mergeFunctionWrapper.add( // 将 previousRow 的记录添加到 mergeFunctionWrapper 中，作为这个分组的第一条记录
                        requireCopy ? previous.getCopiedKv() : previous.getReusedKv());

                // 2. 组内合并
                while (readOnce()) { // while (readOnce()) 循环，不断从 kvIter 读取下一条记录到 currentRow
                    if (keyComparator.compare( // 使用 keyComparator 比较 previousRow.key 和 currentRow.key
                                    previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break; // 如果 key 不同：说明当前分组已结束。break 内部循环。此时的 currentRow 已经是下一个分组的第一条记录了。
                    }
                    // 如果 key 相同：说明仍在同一个分组内。将 currentRow 添加到 mergeFunctionWrapper，然后再次调用 swapSerializers()，让 currentRow 成为新的 previousRow，为比较下一条记录做准备
                    mergeFunctionWrapper.add(
                            requireCopy ? current.getCopiedKv() : current.getReusedKv());
                    swapSerializers();
                }
                // 跳出内部循环后，调用 mergeFunctionWrapper.getResult() 获取整个分组的合并结果，并存入 result 字段
                result = mergeFunctionWrapper.getResult();
            } while (result == null); // 处理空结果：外部的 do-while (result == null) 循环非常重要。有些合并逻辑可能会产生空结果（例如，+I 和 -D 记录合并后互相抵消）。这个循环确保迭代器会继续前进，直到找到一个有效的合并结果或者迭代结束
        }

        private boolean readOnce() throws IOException {
            try {
                currentRow = kvIter.next(currentRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow);
                if (rawConsumer != null) {
                    rawConsumer.accept(current.getReusedKv());
                }
            }
            return currentRow != null;
        }

        private void swapSerializers() {
            KeyValueSerializer tmp = previous;
            BinaryRow tmpRow = previousRow;
            previous = current;
            previousRow = currentRow;
            current = tmp;
            currentRow = tmpRow;
        }
    }
}
