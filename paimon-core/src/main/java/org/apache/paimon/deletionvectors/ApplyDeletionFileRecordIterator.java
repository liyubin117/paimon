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
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link FileRecordIterator} wraps a {@link FileRecordIterator} and {@link DeletionVector}.
 * dv模式的迭代读取逻辑：
 * 只会看到那些未被标记为删除的记录
 * 被删除的记录会被透明地过滤掉
 * */
public class ApplyDeletionFileRecordIterator
        implements FileRecordIterator<InternalRow>, DeletionFileRecordIterator {

    private final FileRecordIterator<InternalRow> iterator;
    private final DeletionVector deletionVector;

    public ApplyDeletionFileRecordIterator(
            FileRecordIterator<InternalRow> iterator, DeletionVector deletionVector) {
        this.iterator = iterator;
        this.deletionVector = deletionVector;
    }

    @Override
    public FileRecordIterator<InternalRow> iterator() {
        return iterator;
    }

    @Override
    public DeletionVector deletionVector() {
        return deletionVector;
    }

    @Override
    public long returnedPosition() {
        return iterator.returnedPosition();
    }

    @Override
    public Path filePath() {
        return iterator.filePath();
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        while (true) {
            InternalRow next = iterator.next();  // 从底层迭代器获取下一条记录
            if (next == null) {
                return null;  // 没有更多记录
            }
            // 检查当前记录的位置是否在删除向量中标记为已删除
            if (!deletionVector.isDeleted(returnedPosition())) {
                return next;  // 如果未被删除，则返回该记录
            }
            // 如果被删除，则继续循环获取下一条记录
        }
    }

    @Override
    public void releaseBatch() {
        iterator.releaseBatch();
    }
}
