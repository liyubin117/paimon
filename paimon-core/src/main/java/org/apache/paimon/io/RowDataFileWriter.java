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
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 *
 * 专门用于将 InternalRow 写入数据文件，写入完成后生成 DataFileMeta（包含文件路径、大小、记录数和统计信息等元数据）
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId; // 数据 schema 版本 ID
    private final LongCounter seqNumCounter; // 序列号计数器，用于跟踪写入记录数
    private final boolean isExternalPath; //是否为外部存储路径
    private final SimpleStatsConverter statsArraySerializer; // 统计信息序列化器
    @Nullable private final DataFileIndexWriter dataFileIndexWriter; // 数据文件索引写入器（可为 null）
    private final FileSource fileSource; // 文件来源元数据
    @Nullable private final List<String> writeCols;

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            LongCounter seqNumCounter,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols) {
        super(fileIO, context, path, Function.identity(), writeSchema, asyncFileWrite);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounter;
        this.isExternalPath = isExternalPath;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema, statsDenseStore);
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), writeSchema, fileIndexOptions);
        this.fileSource = fileSource;
        this.writeCols = writeCols;
    }

    /**
     * 1.调用父类方法写入原始数据
     * 2.通过 dataFileIndexWriter 写入索引信息（与之前分析的 IndexMaintainer 关联）
     * 3.递增序列号计数器
     */
    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(row);
        }
        seqNumCounter.add(1L);
    }

    /**
     * 先关闭索引写入器，再关闭父类资源
     */
    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }

    /**
     * 收集文件统计信息（大小、记录数、索引数据）
     * 生成 DataFileMeta 对象，包含：
     *  文件基本信息（名称、大小、记录数）
     *  统计信息（通过 SimpleStatsConverter 序列化）
     *  索引信息（独立索引文件路径或内嵌索引字节）
     *  序列号范围（起始/结束序列号）
     *  schema 版本和文件来源信息
     */
    @Override
    public DataFileMeta result() throws IOException {
        long fileSize = outputBytes;
        // 调用 fieldStats()，将得到的统计信息 SimpleColStats[] 序列化后，与其他文件元数据（如行数、schema ID 等）一同封装到 DataFileMeta 对象中，最终记录在 Paimon 的 manifest 文件里，为后续的查询优化（如谓词下推）提供数据支持
        Pair<List<String>, SimpleStats> statsPair =
                statsArraySerializer.toBinary(fieldStats(fileSize));

        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();
        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                recordCount(),
                statsPair.getRight(),
                seqNumCounter.getValue() - super.recordCount(),
                seqNumCounter.getValue() - 1,
                schemaId,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                indexResult.embeddedIndexBytes(),
                fileSource,
                statsPair.getKey(),
                externalPath,
                null,
                writeCols);
    }
}
