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

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels.PositionedKeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 在合并（Compaction）过程中，为涉及 L0 文件的 key 生成正确的 Changelog（INSERT, DELETE, UPDATE_BEFORE/AFTER），并处理删除操作
 * <p>Wrapper for {@link MergeFunction}s to produce changelog by lookup during the compaction involving
 * level 0 files.
 *
 * <p>Changelog records are generated in the process of the level-0 file participating in the
 * compaction, if during the compaction processing:
 *
 * <ul>
 *   <li>Without level-0 records, no changelog.
 *   <li>With level-0 record, with level-x (x > 0) record, level-x record should be BEFORE, level-0
 *       should be AFTER.
 *   <li>With level-0 record, without level-x record, need to lookup the history value of the upper
 *       level as BEFORE.
 * </ul>
 */
public class LookupChangelogMergeFunctionWrapper<T>
        implements MergeFunctionWrapper<ChangelogResult> {

    private final LookupMergeFunction mergeFunction; // 负责对同一个主键的所有待合并记录（candidates）进行初步处理，其核心方法 pickHighLevel()用于从这些待合并记录中，找出已经存在于本次合并的高层级(level>0)的层数最小的那一条
    private final Function<InternalRow, T> lookup; // 查找函数。当 mergeFunction在当前待合并的记录中找不到高层级的旧记录时，就会调用这个 lookup函数去所有更高层级的文件中进行外部查找。这是实现跨层级操作的关键

    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();
    @Nullable private final RecordEqualiser valueEqualiser;
    private final LookupStrategy lookupStrategy;
    private final @Nullable BucketedDvMaintainer deletionVectorsMaintainer; // dv索引维护者
    private final Comparator<KeyValue> comparator;

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            @Nullable RecordEqualiser valueEqualiser,
            LookupStrategy lookupStrategy,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "Merge function should be a LookupMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        if (lookupStrategy.deletionVector) {
            checkArgument(
                    deletionVectorsMaintainer != null,
                    "deletionVectorsMaintainer should not be null, there is a bug.");
        }
        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.lookupStrategy = lookupStrategy;
        this.deletionVectorsMaintainer = deletionVectorsMaintainer;
        this.comparator = createSequenceComparator(userDefinedSeqComparator);
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /**
     * 清晰地展示了 Paimon 的 Compaction 机制：
     * 1. 优先使用当前合并单元内的信息拿到旧值 (pickHighLevel)。
     * 2. 如果信息不足，则通过 lookup查找更高层级、未参与合并的文件。
     * 3. 当 lookup找到需要被更新或删除的旧数据时，如果该数据所在的文件不被重写，就调用 deletionVectorsMaintainer.notifyNewDeletion()来记录一个删除标记。
     * 4. 这个删除标记最终会被 dvMaintainer写入到索引文件中，并在查询时调用 deletionVectorsMaintainer.deletionVectorOf。
     */
    @Override
    public ChangelogResult getResult() {
        // 1. Find the latest high level record and compute containLevel0
        KeyValue highLevel = mergeFunction.pickHighLevel(); // 从这些候选kv中，找到层数最小（level 数字最小且 > 0）的那条记录。这条记录就是这个主键在本次合并范围内的“旧值”
        boolean containLevel0 = mergeFunction.containLevel0();

        // 2. 如果找不到旧值，意味着当前合并的所有文件中，要么只有 L0 的记录，要么根本没有这个 key 的记录。则进行外部lookup
        if (highLevel == null) {
            T lookupResult = lookup.apply(mergeFunction.key()); // 去未参与未次合并的、更高层级文件中查找此key
            if (lookupResult != null) {
                if (lookupStrategy.deletionVector) { // 若在更高层级找到了旧值且开启了dv
                    PositionedKeyValue positionedKeyValue = (PositionedKeyValue) lookupResult; // 包装，不仅包含此旧值的kv，还包含其物理位置（文件名、行号）
                    highLevel = positionedKeyValue.keyValue();
                    deletionVectorsMaintainer.notifyNewDeletion( // 关键逻辑，通知 dvMaintainer，“请在 fileName这个文件的 rowPosition这一行上，标记一个删除位”
                            positionedKeyValue.fileName(), positionedKeyValue.rowPosition());
                } else {
                    highLevel = (KeyValue) lookupResult;
                }
            }
            if (highLevel != null) {
                mergeFunction.insertInto(highLevel, comparator);
            }
        }

        // 3. Calculate result
        KeyValue result = mergeFunction.getResult();

        // 4. Set changelog when there's level-0 records
        // 将所有记录（包括 L0 的新记录和找到的 highLevel旧记录）交给底层的 mergeFunction计算出最终结果 result。然后根据 highLevel(旧值) 和 result(新值) 生成 Changelog
        reusedResult.reset();
        if (containLevel0 && lookupStrategy.produceChangelog) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result);
    }

    /**
     * 如果 highLevel存在，result是新值，就会生成 UPDATE_BEFORE和 UPDATE_AFTER。如果 highLevel不存在，result是新值，就会生成 INSERT
     */
    private void setChangelog(@Nullable KeyValue before, KeyValue after) {
        if (before == null || !before.isAdd()) {
            if (after.isAdd()) {
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after));
            }
        } else {
            if (!after.isAdd()) {
                reusedResult.addChangelog(replaceBefore(RowKind.DELETE, before));
            } else if (valueEqualiser == null
                    || !valueEqualiser.equals(before.value(), after.value())) {
                reusedResult
                        .addChangelog(replaceBefore(RowKind.UPDATE_BEFORE, before))
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after));
            }
        }
    }

    private KeyValue replaceBefore(RowKind valueKind, KeyValue from) {
        return replace(reusedBefore, valueKind, from);
    }

    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber);
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o1.value(), o2.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
        };
    }
}
