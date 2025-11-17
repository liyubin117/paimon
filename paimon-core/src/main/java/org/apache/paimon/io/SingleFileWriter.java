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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.AsyncPositionOutputStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * A {@link FileWriter} to produce a single file.
 *
 * 负责将数据写入单个物理文件的抽象基类。它是 Paimon I/O 栈中非常基础且核心的一个组件，封装了文件创建、数据写入、关闭、异常处理和回滚等通用逻辑，使得上层应用（如 MergeTreeWriter 或 AppendOnlyWriter）可以专注于业务逻辑，而无需关心底层文件格式（Parquet, ORC, Avro）和文件系统（HDFS, S3, Local）的细节
 *
 * @param <T> type of records to write.
 * @param <R> type of result to produce after writing a file.
 */
public abstract class SingleFileWriter<T, R> implements FileWriter<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleFileWriter.class);

    protected final FileIO fileIO; // 文件系统接口，用于与底层存储（如 HDFS, S3）交互，负责创建输出流和删除文件
    protected final Path path; // 明确指定了要写入的目标文件的完整路径
    private final Function<T, InternalRow> converter; // 一个转换函数。因为 Paimon 底层的 FormatWriter 只接受 InternalRow 格式，而上层传入的数据类型可能是 KeyValue 或其他自定义类型 T。这个 converter 的作用就是将泛型 T 转换为标准的 InternalRow

    private FormatWriter writer; // 由 FormatWriterFactory 创建，是真正负责将 InternalRow 按照特定文件格式（如 Parquet, ORC）序列化并写入 out 输出流的组件
    private PositionOutputStream out; // 从 fileIO 获取的原始文件输出流。它支持获取当前写入位置（getPos()），这对于记录文件大小很重要

    protected long outputBytes;
    private long recordCount; // 用于追踪写入状态
    protected boolean closed;

    public SingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            String compression,
            boolean asyncWrite) {
        this.fileIO = fileIO;
        this.path = path;
        this.converter = converter;

        try {
            if (factory instanceof SupportsDirectWrite) {
                writer = ((SupportsDirectWrite) factory).create(fileIO, path, compression);
            } else {
                // 1. 从文件系统获取输出流
                out = fileIO.newOutputStream(path, false);
                // 2. 如果需要，包装成异步输出流
                if (asyncWrite) {
                    out = new AsyncPositionOutputStream(out);
                }
                // 3. 通过工厂创建具体格式的写入器
                writer = factory.create(out, compression);
            }
        } catch (IOException e) {
            LOG.warn(
                    "Failed to open the bulk writer, closing the output stream and throw the error.",
                    e);
            if (out != null) {
                abort();
            }
            throw new UncheckedIOException(e);
        }

        this.recordCount = 0;
        this.closed = false;
    }

    public Path path() {
        return path;
    }

    @Override
    public void write(T record) throws IOException {
        writeImpl(record);
    }

    public void writeBundle(BundleRecords bundle) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            if (writer instanceof BundleFormatWriter) {
                ((BundleFormatWriter) writer).writeBundle(bundle);
            } else {
                for (InternalRow row : bundle) {
                    writer.addElement(row);
                }
            }
            recordCount += bundle.rowCount();
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file " + path + ". Cleaning up.", e);
            abort();
            throw e;
        }
    }

    protected InternalRow writeImpl(T record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            // 1. 将输入记录转换为 InternalRow
            InternalRow rowData = converter.apply(record);
            // 2. 调用 FormatWriter 写入元素
            writer.addElement(rowData);
            recordCount++;
            return rowData;
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file " + path + ". Cleaning up.", e);
            // 3. 如果写入失败，立即中止
            abort();
            throw e;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    /**
     * 回滚，删除文件
     */
    @Override
    public void abort() {
        if (writer != null) {
            IOUtils.closeQuietly(writer);
            writer = null;
        }
        if (out != null) {
            IOUtils.closeQuietly(out);
            out = null;
        }
        fileIO.deleteQuietly(path);
    }

    public AbortExecutor abortExecutor() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed!");
        }

        return new AbortExecutor(fileIO, path);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file {}", path);
        }

        try {
            if (writer != null) {
                // 1. 关闭 FormatWriter，它会刷写所有缓冲区，并写入文件尾（如 Parquet 的 footer）
                writer.close();
                writer = null;
            }
            if (out != null) {
                // 2. 刷写并关闭文件输出流
                out.flush();
                outputBytes = out.getPos();
                out.close();
                out = null;
            }
        } catch (IOException e) {
            // ... 异常处理，如果关闭失败，还需要回滚 ...
            LOG.warn("Exception occurs when closing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    /** Abort executor to just have reference of path instead of whole writer. */
    public static class AbortExecutor {

        private final FileIO fileIO;
        private final Path path;

        private AbortExecutor(FileIO fileIO, Path path) {
            this.fileIO = fileIO;
            this.path = path;
        }

        public void abort() {
            fileIO.deleteQuietly(path);
        }
    }
}
