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
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * A {@link SingleFileWriter} which also produces statistics for each written field.
 * 一个在写入单个文件的同时包装了收集统计信息的写入器，写入数据文件时收集列统计信息（如最大值、最小值、空值计数等）的核心抽象类
 *
 * @param <T> type of records to write.
 * @param <R> type of result to produce after writing a file.
 */
public abstract class StatsCollectingSingleFileWriter<T, R> extends SingleFileWriter<T, R> {

    private final RowType rowType; // 正在写入的数据的 schema（模式）。统计信息是按列收集的，因此必须知道数据的行类型结构
    private final SimpleStatsProducer statsProducer; // 决定了统计信息是以何种方式被生产出来的，两种：1.逐条记录 2.文件footer提取
    private final boolean isStatsDisabled; // 标志，用于快速判断当前是否禁用了统计信息收集
    private final boolean statsRequirePerRecord; // 标志，用于判断 statsProducer 采用的是哪种策略。如果为 true，则必须逐条记录收集

    public StatsCollectingSingleFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<T, InternalRow> converter,
            RowType rowType,
            boolean asyncWrite) {
        super(fileIO, context.factory(), path, converter, context.compression(), asyncWrite);
        this.rowType = rowType;
        /**
         * 在上层逻辑中根据文件格式（FileFormat）和用户配置（stats.mode）来决定
         * - fromCollector 对于 Avro 格式，由于其本身不存储列统计信息，Paimon 必须在写入时逐条计算。此时会创建一个基于 SimpleStatsCollector 的 statsProducer，其 statsRequirePerRecord 为 true。
         * - fromExtractor 对于 Parquet 或 ORC 格式，它们可以在文件尾部记录统计信息。Paimon 可以利用这一点，在文件写完后直接从文件元数据中解析。此时会创建一个基于 SimpleStatsExtractor 的 statsProducer，其 statsRequirePerRecord 为 false
         */
        this.statsProducer = context.statsProducer();
        this.isStatsDisabled = statsProducer.isStatsDisabled();
        this.statsRequirePerRecord = statsProducer.requirePerRecord();
    }

    @Override
    public void write(T record) throws IOException {
        InternalRow rowData = writeImpl(record); // 调用父类 SingleFileWriter 的方法，将记录实际写入文件
        if (!isStatsDisabled && statsRequirePerRecord) { // 只有在启用统计功能并要求逐条记录时，进行下一步
            statsProducer.collect(rowData); // 将当前写入的行数据传递给它，用于实时更新统计值（min/max/nullCount）
        }
    }

    /**
     * 用于优化，可以一次性写入一批预序列化的记录
     */
    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // 首先检查 statsRequirePerRecord。如果为 true，意味着统计信息需要从每一条独立的记录中提取，而 BundleRecords 是一个整体，无法逐条解析。在这种情况下，为了防止丢失统计信息，会直接抛出异常
        if (statsRequirePerRecord) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't write bundle for %s, we may lose all the statistical information.",
                            statsProducer.getClass().getName()));
        }
        // statsRequirePerRecord 为 false（例如使用 Parquet/ORC 格式），则可以安全地使用 writeBundle，因为统计信息最终会从文件尾部提取，与单条记录如何写入无关
        super.writeBundle(bundle);
    }

    /**
     * 当文件写入完成并关闭后，此方法用于获取最终的统计结果
     */
    public SimpleColStats[] fieldStats(long fileSize) throws IOException {
        // 确保该方法只在文件写入器关闭后调用
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");
        // 如果禁用了统计，直接为每一列返回一个空的统计对象 SimpleColStats.NONE
        if (isStatsDisabled) {
            return IntStream.range(0, rowType.getFieldCount())
                    .mapToObj(i -> SimpleColStats.NONE)
                    .toArray(SimpleColStats[]::new);
        }
        /**
         * 获取结果的统一出口。
         * 如果之前是逐条收集的策略，extract 方法会从内部的 SimpleStatsCollector 中提取出最终累积的结果。
         * 如果之前是文件末端提取的策略，extract 方法会真正地打开文件（path），读取其元数据，并解析出统计信息
         */
        return statsProducer.extract(fileIO, path, fileSize);
    }
}
