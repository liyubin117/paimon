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

package org.apache.paimon.format.parquet;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.parquet.ParquetFileFormatFactory.IDENTIFIER;

/** Parquet {@link FileFormat}.
 * 宏观上按行分组成行组 (Row Group)，这是读写的基本单元，便于数据并行处理和管理。
 * 行组内部按列组织成列块 (Column Chunk)，这是列式存储的核心，实现了按需读取。
 * 列块内部按页组织成数据页 (Data Page)，这是压缩和编码的基本单元，极致地优化了存储效率。
 * 丰富的元数据和统计信息，贯穿于file(row group)、column chunk、page header三个层级，为查询优化（如谓词下推）提供了强大的支持
 * */
public class ParquetFileFormat extends FileFormat {

    private final Options options;
    private final int readBatchSize;

    public ParquetFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);

        this.options = getParquetConfiguration(formatContext);
        this.readBatchSize = formatContext.readBatchSize();
    }

    @VisibleForTesting
    Options getOptions() {
        return options;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new ParquetReaderFactory(
                options, projectedRowType, readBatchSize, ParquetFilters.convert(filters));
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new ParquetWriterFactory(new RowDataParquetBuilder(type, options));
    }

    @Override
    public void validateDataFields(RowType rowType) {
        ParquetSchemaConverter.convertToParquetMessageType(rowType);
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new ParquetSimpleStatsExtractor(type, statsCollectors));
    }

    private Options getParquetConfiguration(FormatContext context) {
        Options parquetOptions = getIdentifierPrefixOptions(context.options());

        if (!parquetOptions.containsKey("parquet.compression.codec.zstd.level")) {
            parquetOptions.set(
                    "parquet.compression.codec.zstd.level", String.valueOf(context.zstdLevel()));
        }

        MemorySize blockSize = context.blockSize();
        if (blockSize != null) {
            parquetOptions.set(
                    ParquetOutputFormat.BLOCK_SIZE, String.valueOf(blockSize.getBytes()));
        }

        return parquetOptions;
    }
}
