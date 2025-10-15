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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files by lookup for the compaction
 * involving level 0 files.
 * 高级合并器，通过 Lookup 机制同时支持高效生成 Changelog 和协作处理删除向量
 * 适用于changelog-producer = 'lookup'或 'input'的场景，以及启用删除向量的场景
 * dv模式的核心组件，负责合并低层级（比如 L0）的文件。查找并标记删除，对于 L0 文件中的每一条记录，它需要去更高层级（L1, L2...）的文件中查找是否存在对应的旧记录
 *
 * 假设正在合并 L0 的文件，其中有一条 DELETE记录，主键为 pk=1。
     * LookupMergeTreeCompactRewriter会使用 LookupLevels去更高层级（L1, L2...）查找 pk=1。
     * LookupLevels找到了 pk=1的旧记录，它位于 L2 层的一个叫 data-file-A.orc的文件中，行号是 100。
     * 因为这次 Compaction 只合并 L0 的文件，data-file-A.orc文件不会被重写。
     * 为了让这条 DELETE记录生效，Rewriter 必须想办法标记 data-file-A.orc的第 100 行为“已删除”。
     * 这就是 dvMaintainer发挥作用的时刻！
         * Rewriter 会调用 dvMaintainer.notifyNewDeletion("data-file-A.orc", 100)。
         * 该标记最终写入独立的删除向量索引文件，并在查询时过滤已删除行
 */
public class LookupMergeTreeCompactRewriter<T> extends ChangelogMergeTreeRewriter {

    private final LookupLevels<T> lookupLevels;
    private final MergeFunctionWrapperFactory<T> wrapperFactory;
    private final boolean noSequenceField;
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    private final IntFunction<String> level2FileFormat;

    public LookupMergeTreeCompactRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            LookupLevels<T> lookupLevels,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            MergeFunctionWrapperFactory<T> wrapperFactory,
            boolean produceChangelog,
            @Nullable BucketedDvMaintainer dvMaintainer,
            CoreOptions options) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                produceChangelog,
                dvMaintainer != null);
        this.dvMaintainer = dvMaintainer;
        this.lookupLevels = lookupLevels;
        this.wrapperFactory = wrapperFactory;
        this.noSequenceField = options.sequenceField().isEmpty();
        String fileFormat = options.fileFormatString();
        Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
        this.level2FileFormat = level -> fileFormatPerLevel.getOrDefault(level, fileFormat);
    }

    @Override
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {
        if (dvMaintainer != null) {
            files.forEach(file -> dvMaintainer.removeDeletionVectorOf(file.fileName()));
        }
    }

    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        return rewriteLookupChangelog(outputLevel, sections);
    }

    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        if (file.level() != 0) {
            return NO_CHANGELOG_NO_REWRITE;
        }

        // forcing rewriting when upgrading from level 0 to level x with different file formats
        if (!level2FileFormat.apply(file.level()).equals(level2FileFormat.apply(outputLevel))) {
            return CHANGELOG_WITH_REWRITE;
        }

        // In deletionVector mode, since drop delete is required, when delete row count > 0 rewrite
        // is required.
        if (dvMaintainer != null && file.deleteRowCount().map(cnt -> cnt > 0).orElse(true)) {
            return CHANGELOG_WITH_REWRITE;
        }

        if (outputLevel == maxLevel) {
            return CHANGELOG_NO_REWRITE;
        }

        // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
        // it at all.
        if (mergeEngine == MergeEngine.DEDUPLICATE && noSequenceField) {
            return CHANGELOG_NO_REWRITE;
        }

        // other merge engines must rewrite file, because some records that are already at higher
        // level may be merged
        // See LookupMergeFunction, it just returns newly records.
        return CHANGELOG_WITH_REWRITE;
    }

    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return wrapperFactory.create(mfFactory, outputLevel, lookupLevels, dvMaintainer);
    }

    @Override
    public void close() throws IOException {
        lookupLevels.close();
    }

    /** Factory to create {@link MergeFunctionWrapper}. */
    public interface MergeFunctionWrapperFactory<T> {

        MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer);
    }

    /** A normal {@link MergeFunctionWrapperFactory} to create lookup wrapper. */
    public static class LookupMergeFunctionWrapperFactory<T>
            implements MergeFunctionWrapperFactory<T> {

        @Nullable private final RecordEqualiser valueEqualiser;
        private final LookupStrategy lookupStrategy;
        @Nullable private final UserDefinedSeqComparator userDefinedSeqComparator;

        public LookupMergeFunctionWrapperFactory(
                @Nullable RecordEqualiser valueEqualiser,
                LookupStrategy lookupStrategy,
                @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
            this.valueEqualiser = valueEqualiser;
            this.lookupStrategy = lookupStrategy;
            this.userDefinedSeqComparator = userDefinedSeqComparator;
        }

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            // 调用LookupChangelogMergeFunctionWrapper，是执行“查找并标记删除”这一核心逻辑的关键组件
            return new LookupChangelogMergeFunctionWrapper<>(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    valueEqualiser,
                    lookupStrategy,
                    deletionVectorsMaintainer,
                    userDefinedSeqComparator);
        }
    }

    /** A {@link MergeFunctionWrapperFactory} for first row. */
    public static class FirstRowMergeFunctionWrapperFactory
            implements MergeFunctionWrapperFactory<Boolean> {

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<Boolean> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            return new FirstRowMergeFunctionWrapper(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1) != null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
