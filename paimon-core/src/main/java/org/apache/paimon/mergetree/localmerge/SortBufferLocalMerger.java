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

package org.apache.paimon.mergetree.localmerge;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import java.io.IOException;
import java.util.function.Consumer;

/** A {@link LocalMerger} which stores records in {@link SortBufferWriteBuffer}.
 * 在数据被 Shuffle 到不同的 Bucket 之前，在 Flink 的 Source 端或者中间算子内部，对数据进行一次本地的预合并（Pre-aggregation）
 * 目的非常明确：缓解数据倾斜
 *      如果没有 Local Merge: 这成千上万条记录会被原封不动地通过网络 Shuffle 到下游负责该 Key 所在 Bucket 的 MergeTreeWriter 任务中。这会造成巨大的网络开销，并且给下游的单个 MergeTreeWriter 带来巨大的写入压力。
 *      有了 Local Merge: SortBufferLocalMerger 会在发送端开辟一块内存缓冲区（由 local-merge-buffer-size 参数控制）。当这成千上万条记录到达时，它们会被缓存在这个 Buffer 中，并利用 SortBufferWriteBuffer 的能力进行排序和合并。最终，可能只有一条合并后的记录被发送到下游。
 *
 * 通过 local-merge-buffer-size 这个配置项来决定是否启用 Local Merge
 * */
public class SortBufferLocalMerger implements LocalMerger {

    private final SortBufferWriteBuffer sortBuffer;
    private final RecordComparator keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;

    private long recordCount;

    public SortBufferLocalMerger(
            SortBufferWriteBuffer sortBuffer,
            RecordComparator keyComparator,
            MergeFunction<KeyValue> mergeFunction) {
        this.sortBuffer = sortBuffer;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.recordCount = 0;
    }

    @Override
    public boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException {
        return sortBuffer.put(recordCount++, rowKind, key, value);
    }

    @Override
    public int size() {
        return sortBuffer.size();
    }

    @Override
    public void forEach(Consumer<InternalRow> consumer) throws IOException {
        sortBuffer.forEach(
                keyComparator,
                mergeFunction,
                null,
                kv -> {
                    InternalRow row = kv.value();
                    row.setRowKind(kv.valueKind());
                    consumer.accept(row);
                });
    }

    @Override
    public void clear() {
        sortBuffer.clear();
    }
}
