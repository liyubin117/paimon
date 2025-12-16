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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A class which stores all level files of merge tree.
 * MergeTree 结构中用于组织和管理数据文件元数据（DataFileMeta）的核心组件。
 * 将数据文件按照其 "level"（层级）进行分层存储，这对于 LSM-Tree (Log-Structured Merge-Tree) 类似的结构非常重要，有助于优化读写性能和 Compaction 过程。
 * 职责：
 *  分层组织数据：将数据文件按层级划分，便于管理和优化。
 *  支持高效查找：通过分层和排序，可以加速键的查找过程。查找通常从 Level 0 开始，逐层向上查找，直到找到目标键或确定键不存在。
 *  支持 Compaction：Levels 的结构是 Compaction 策略的基础。Compaction 过程会选择某些层级的文件进行合并，将结果写入到更高层级，以减少文件数量、消除冗余数据、提高读取效率。
 *  元数据管理：Levels.update(List<DataFileMeta> before, List<DataFileMeta> after) 方法用于更新 Levels 中管理的 DataFileMeta 集合，反映表快照的变化。
 * */
public class Levels {
    // 键比较器，用于比较数据行中的键，这在构建 SortedRun、查找以及合并过程中至关重要
    private final Comparator<InternalRow> keyComparator;
    // 数据最新写入的层级，通常包含较小且可能重叠的文件。使用 TreeSet 可以根据文件的某些属性（例如，序列号或文件名，取决于比较器的实现，现有Paimon实现是序号降序，名字升序）保持有序，这有助于合并和查找
    private final TreeSet<DataFileMeta> level0;
    // levels 列表中的每个元素代表一个更高层级（Level 1, Level 2, ...）。列表的索引对应层级 - 1
    // 每个 SortedRun 对象内部包含一组 DataFileMeta，这些文件在该层级内通常是根据主键排序且不重叠的
    private final List<SortedRun> levels;
    // 当 Levels.update() 方法移除某些 DataFileMeta 时，会触发这些回调。例如，LookupLevels 类实现了 DropFileCallback 接口，用于在其管理的 DataFileMeta 被移除时，清理相关的缓存（如本地的 lookup file）
    private final List<DropFileCallback> dropFileCallbacks = new ArrayList<>();

    /**
     *
     * @param keyComparator
     * @param inputFiles
     * @param numLevels 最大层级数
     */
    public Levels(
            Comparator<InternalRow> keyComparator, List<DataFileMeta> inputFiles, int numLevels) {
        this.keyComparator = keyComparator;

        // in case the num of levels is not specified explicitly
        int restoredNumLevels =
                Math.max(
                        numLevels,
                        inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(-1) + 1);
        checkArgument(restoredNumLevels > 1, "Number of levels must be at least 2.");
        this.level0 =
                new TreeSet<>(
                        (a, b) -> {
                            if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
                                // file with larger sequence number should be in front
                                return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
                            } else {
                                // When two or more jobs are writing the same merge tree, it is
                                // possible that multiple files have the same maxSequenceNumber. In
                                // this case we have to compare their file names so that files with
                                // same maxSequenceNumber won't be "de-duplicated" by the tree set.
                                int minSeqCompare =
                                        Long.compare(a.minSequenceNumber(), b.minSequenceNumber());
                                if (minSeqCompare != 0) {
                                    return minSeqCompare;
                                }
                                // If minSequenceNumber is also the same, use creation time
                                int timeCompare = a.creationTime().compareTo(b.creationTime());
                                if (timeCompare != 0) {
                                    return timeCompare;
                                }
                                // Final fallback: filename (to ensure uniqueness in TreeSet)
                                return a.fileName().compareTo(b.fileName());
                            }
                        });
        this.levels = new ArrayList<>();
        for (int i = 1; i < restoredNumLevels; i++) {
            levels.add(SortedRun.empty());
        }

        Map<Integer, List<DataFileMeta>> levelMap = new HashMap<>();
        for (DataFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));

        Preconditions.checkState(
                level0.size() + levels.stream().mapToInt(r -> r.files().size()).sum()
                        == inputFiles.size(),
                "Number of files stored in Levels does not equal to the size of inputFiles. This is unexpected.");
    }

    public TreeSet<DataFileMeta> level0() {
        return level0;
    }

    public void addDropFileCallback(DropFileCallback callback) {
        dropFileCallbacks.add(callback);
    }

    public void addLevel0File(DataFileMeta file) {
        checkArgument(file.level() == 0);
        level0.add(file);
    }

    public SortedRun runOfLevel(int level) {
        checkArgument(level > 0, "Level0 does not have one single sorted run.");
        return levels.get(level - 1);
    }

    public int numberOfLevels() {
        return levels.size() + 1;
    }

    public int maxLevel() {
        return levels.size();
    }

    public int numberOfSortedRuns() {
        int numberOfSortedRuns = level0.size();
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++;
            }
        }
        return numberOfSortedRuns;
    }

    /** @return the highest non-empty level or -1 if all levels empty. */
    public int nonEmptyHighestLevel() {
        int i;
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        return level0.isEmpty() ? -1 : 0;
    }

    public long totalFileSize() {
        return level0.stream().mapToLong(DataFileMeta::fileSize).sum()
                + levels.stream().mapToLong(SortedRun::totalSize).sum();
    }

    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        for (int i = 0; i < levels.size(); i++) {
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        if (dropFileCallbacks.size() > 0) {
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
            // exclude upgrade files
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);
            for (DropFileCallback callback : dropFileCallbacks) {
                droppedFiles.forEach(callback::notifyDropFile);
            }
        }
    }

    private void updateLevel(int level, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (before.isEmpty() && after.isEmpty()) {
            return;
        }

        if (level == 0) {
            before.forEach(level0::remove);
            level0.addAll(after);
        } else {
            List<DataFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            files.removeAll(before);
            files.addAll(after);
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator));
        }
    }

    private Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }

    /** A callback to notify dropping file. */
    public interface DropFileCallback {

        void notifyDropFile(String file);
    }
}
