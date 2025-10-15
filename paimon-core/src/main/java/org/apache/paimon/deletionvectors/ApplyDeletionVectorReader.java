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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} which apply {@link DeletionVector} to filter record.
 * Deletion Vector (DV) 的目的是在不重写数据文件的情况下，标记其中的某些行已被删除。这对于 partial-update 等需要先删除旧值再插入新值的场景至关重要。
 * */
public class ApplyDeletionVectorReader implements FileRecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> reader;

    private final DeletionVector deletionVector;

    public ApplyDeletionVectorReader(
            FileRecordReader<InternalRow> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    public RecordReader<InternalRow> reader() {
        return reader;
    }

    public DeletionVector deletionVector() {
        return deletionVector;
    }

    /**
     * 当调用 readBatch()方法时：
     * 1. 从底层的 FileRecordReader获取一批记录
     * 2. 返回一个 ApplyDeletionFileRecordIterator实例来迭代这些记录
     */
    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        return new ApplyDeletionFileRecordIterator(batch, deletionVector);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
