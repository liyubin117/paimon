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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;

/** Sort buffer to sort records.
 * 用于处理排序的核心抽象接口。它的主要职责是：接收一批数据记录（InternalRow），在内存中对它们进行排序，并提供一个有序的迭代器来访问这些排好序的数据。 当内存不足以容纳所有数据时，它还隐含了将数据溢出（Spill）到磁盘的能力
 * 这种设计使得上层逻辑（如 SortOperator、SortBufferWriteBuffer）可以不必关心排序的具体细节（是纯内存还是溢出到磁盘），从而实现了清晰的分层和强大的功能扩展。
 * */
public interface SortBuffer {

    /** 返回当前 buffer 中的记录数 */
    int size();

    /** 清空 buffer，释放所有资源，使其可以被重用 */
    void clear();

    /** 获取当前 buffer 占用的内存大小（字节）*/
    long getOccupancy();

    /**
     * 尝试将内存中的数据刷写到外部存储（如磁盘）。
     * 这是为外部排序设计的关键方法。如果不支持（例如纯内存排序），则返回 false。
     */
    boolean flushMemory() throws IOException;

    /**
     * 向 buffer 中写入一条记录。
     * @return 如果 buffer 已满，无法写入，则返回 false。调用方需要处理这种情况（通常是触发 flush）。
     */
    boolean write(InternalRow record) throws IOException;

    /**
     * 对 buffer 中已写入的所有记录进行排序，并返回一个有序的迭代器。
     * 返回的迭代器中的元素是 BinaryRow，这是一种序列化后的行格式，便于在内存和磁盘间高效传输。
     */
    MutableObjectIterator<BinaryRow> sortedIterator() throws IOException;
}
