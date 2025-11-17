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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.SingleFileWriter.AbortExecutor;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Writer to roll over to a new file if the current size exceed the target file size.
 *
 * 在 SingleFileWriter 的基础上增加了一层滚动（Rolling） 的功能。它的核心作用是：在写入数据时，如果当前文件的大小达到了预设的目标大小，就自动关闭当前文件，并创建一个新的文件继续写入，从而将一个大的数据流切分成多个大小合适的小文件
 *
 * @param <T> record data type.
 * @param <R> the file metadata result.
 */
public class RollingFileWriter<T, R> implements FileWriter<T, List<R>> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);

    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final Supplier<? extends SingleFileWriter<T, R>> writerFactory;
    private final long targetFileSize;
    private final List<AbortExecutor> closedWriters; // 存储所有已关闭写入的AbortExecutor。这是一个内存优化的技巧。当需要中止整个写入过程时，我们只需要调用这些执行器来删除已生成的文件，而不需要在内存中保留完整的 SingleFileWriter 对象，从而避免了内存泄漏
    private final List<R> results; // 收集所有已成功关闭的 SingleFileWriter 返回的结果。因为会生成多个文件，所以结果是一个列表。R 通常是 DataFileMeta

    private SingleFileWriter<T, R> currentWriter = null; // 当前正在写入的 SingleFileWriter 实例。当它为 null 时，表示需要创建一个新的写入器
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriter(
            Supplier<? extends SingleFileWriter<T, R>> writerFactory, long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();
    }

    @VisibleForTesting
    public long targetFileSize() {
        return targetFileSize;
    }

    private boolean rollingFile(boolean forceCheck) throws IOException {
        return currentWriter.reachTargetSize(
                forceCheck || recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    @Override
    public void write(T row) throws IOException {
        try {
            // 1. 如果当前没有写入器，创建一个新的
            if (currentWriter == null) {
                openCurrentWriter();
            }

            // 2. 将数据写入当前的 SingleFileWriter
            currentWriter.write(row);
            recordCount += 1;

            // 3. 检查是否需要滚动
            if (rollingFile(false)) {
                // 4. 如果需要，关闭当前写入器，准备下次写入时创建新的
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            // ... 异常处理，调用 abort() 清理所有文件 ...
            LOG.warn(
                    "Exception occurs when writing file "
                            + (currentWriter == null ? null : currentWriter.path())
                            + ". Cleaning up.",
                    e);
            abort();
            throw e;
        }
    }

    public void writeBundle(BundleRecords bundle) throws IOException {
        try {
            // Open the current writer if write the first record or roll over happen before.
            if (currentWriter == null) {
                openCurrentWriter();
            }

            currentWriter.writeBundle(bundle);
            recordCount += bundle.rowCount();

            if (rollingFile(true)) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing file "
                            + (currentWriter == null ? null : currentWriter.path())
                            + ". Cleaning up.",
                    e);
            abort();
            throw e;
        }
    }

    private void openCurrentWriter() {
        currentWriter = writerFactory.get();
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        // only store abort executor in memory
        // cannot store whole writer, it includes lots of memory for example column vectors to read
        // and write
        closedWriters.add(currentWriter.abortExecutor());
        results.add(currentWriter.result());
        currentWriter = null;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (AbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    @Override
    public List<R> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file " + currentWriter.path() + ". Cleaning up.",
                    e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
