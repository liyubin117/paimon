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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link CompactIncrement}.
 * Paimon写链路的核心，负责一个分区的一个桶内的数据写入、合并、提交
 * 每个分区的每个桶 有 且 只有 一个 MergeTreeWriter
 * MergeTreeWriter 通过 KeyValue 中的 RowKind 来携带增、删、改的语义。在写数据时，它将这些带有语义的记录先放入缓冲区，然后在刷写时，将原始记录流写入 changelog 文件，将合并后的结果写入数据文件。这样既保证了数据文件的紧凑和高效查询，又通过 changelog 文件提供了完整的变更历史
 * 构造中通过 newSequenceNumber = maxSequenceNumber + 1; 尽可能维护统一的序列号
 *
 * 本身只负责生成L0文件，不修改老文件，会调用compaction合并
 * 具体过程：
 *      * 阶段一 (内存/Spill): SortBufferWriteBuffer 接收无序数据，在内存中排序，内存不够时，将临时的、未合并的排好序的数据块溢写到本地临时磁盘，以换取内存。
 *      * 阶段二 (Flush): MergeTreeWriter 命令 SortBufferWriteBuffer 将其管理的所有数据（无论在内存还是在临时磁盘）作为一个全局有序且合并后的数据流提供出来。
 *      * 阶段三 (写入正式文件): MergeTreeWriter 消费这个干净的数据流，将其写入最终的、正式的 Level-0 数据文件（在本地或远程存储），并可能同时生成 Changelog 文件。
 * */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    // ... 内存缓冲与溢写相关配置 ...
    private final boolean writeBufferSpillable;
    private final MemorySize maxDiskSize;
    private final int sortMaxFan;
    private final CompressOptions sortCompression;
    private final IOManager ioManager;

    // ... 核心功能组件 ...
    private final RowType keyType;
    private final RowType valueType;
    private final CompactManager compactManager; // 合并任务管理器。它负责维护该 Bucket 内所有数据文件的层级结构（Levels），并根据策略决定何时、对哪些文件发起 Compaction
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction; // 定义了数据合并的逻辑，对于主键表，它可能是“保留最新的值”（Deduplicate）；对于聚合表，它可能是“对值进行累加”
    private final KeyValueFileWriterFactory writerFactory; // 文件写入工厂，负责创建RollingFileWriter（用于写入SST数据文件和 Changelog 文件）
    private final boolean commitForceCompact;
    private final ChangelogProducer changelogProducer;
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    // ... 状态与结果追踪 ...
    private final LinkedHashSet<DataFileMeta> newFiles; // 记录从内存 writeBuffer 刷盘后生成的新文件
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    // 记录一次 Compaction 操作中，被合并的旧文件和生成的新文件
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    private final LinkedHashSet<DataFileMeta> compactChangelog;
    @Nullable private CompactDeletionFile compactDeletionFile;

    private long newSequenceNumber;

    // ... 内存缓冲区实例 ...
    private WriteBuffer writeBuffer; // 所有新数据首先进入这里进行排序和预合并。setMemoryPool 方法会为其注入内存池

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    public CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        userDefinedSeqComparator,
                        memoryPool,
                        writeBufferSpillable,
                        maxDiskSize,
                        sortMaxFan,
                        sortCompression,
                        ioManager);
    }

    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber = newSequenceNumber();
        // 1. 尝试将数据写入内存缓冲区
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            // 2. 如果内存缓冲区满了，先执行刷盘
            flushWriteBuffer(false, false);
            // 3. 再次尝试写入
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long maxSequenceNumber() {
        return newSequenceNumber - 1;
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    /**
     * 创建 RollingFileWriter，用于写入 Level-0 的数据文件，并根据配置决定是否创建 Changelog 文件写入器。
     * 调用 writeBuffer.forEach()，它会提供一个内存中排序好的数据迭代器。MergeTreeWriter 在遍历时，会应用 mergeFunction 对数据进行最终合并，然后分别写入数据文件和 Changelog 文件。
     * 操作完成后，清空内存缓冲区 writeBuffer，使其可以接收新的数据。
     * 从写入器中获取新生成文件的元数据 DataFileMeta。
     * 将这些元数据记录在 newFiles 集合中，并通知 compactManager 有新的 Level-0 文件加入。
     * compactManager 会根据当前文件层级状态，决定是否需要触发一次新的 Compaction。
     */
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }

            // 1. 创建数据文件和Changelog文件的写入器
            // 职责是创建变更日志文件。这些文件记录了本次提交中每一条原始的变更记录（比如 INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE）。这些文件专门用于支持下游的流式查询任务，让 Flink 等引擎可以像消费 Kafka 一样消费 Paimon 表的增量变化。生成的changelog-前缀的文件不会参与数据合并，它们只是作为本次提交产物的一部分，供流作业消费
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;
            // 职责是创建正式的 Level-0 数据文件（SST 文件）。这些文件是表的核心组成部分，包含了经过排序和合并后的数据，代表了表在某个时间点的最新状态。生成的data-前缀的文件最终被CompactManager管理，参与后续合并
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

            try {
                // 2. 遍历内存缓冲区中的有序数据，应用合并逻辑，并同时写入数据文件和Changelog文件
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        changelogWriter == null ? null : changelogWriter::write,
                        dataWriter::write);
            } finally {
                // 3. 清空内存缓冲区，关闭写入器
                writeBuffer.clear();
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            // 4. 收集新生成的文件元数据
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            }

            // 5. 将新文件信息添加到 newFiles 集合，并通知 compactManager
            for (DataFileMeta fileMeta : dataWriter.result()) {
                newFiles.add(fileMeta); // 进入新文件路径，通过 newFiles -> DataIncrement -> CommitIncrement 的路径，被记录到新的快照中，从而对用户可见
                compactManager.addNewFile(fileMeta); // 进入合并路径，通过 compactManager 进入 LSM 树的管理体系，未来可能会被选中并合并成更大的文件。当它被合并后，它的状态变化（从被合并 -> 产生新文件）会通过 CompactResult -> compactBefore/compactAfter -> CompactIncrement -> CommitIncrement 的路径，再次被记录到新的快照中
            }
        }

        // 6. 检查并触发新的Compaction
        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    // Flink Checkpoint 时被调用的关键方法，完成所有待处理数据的刷盘和合并，并收集本次 Checkpoint 期间文件变动的信息（新增了哪些文件、删除了哪些文件），打包成 CommitIncrement 返回给上层
    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        // 1. 确保内存中的数据全部刷盘
        flushWriteBuffer(waitCompaction, false);
        if (commitForceCompact) {
            waitCompaction = true;
        }
        // Decide again whether to wait here.
        // For example, in the case of repeated failures in writing, it is possible that Level 0
        // files were successfully committed, but failed to restart during the compaction phase,
        // which may result in an increasing number of Level 0 files. This wait can avoid this
        // situation.
        // 当SortedRun的数量>num-sorted-run.stop-trigger，prepareCommit阶段刷写新文件前先要等待compaction防止文件数量过多
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }
        // 2. 同步等待可能正在进行的Compaction任务完成，将其结果（哪些文件被合并，生成了哪些新文件）分别更新到 compactBefore 和 compactAfter 集合中
        trySyncLatestCompaction(waitCompaction);
        // 3. 将 newFiles、compactBefore、compactAfter 等集合中的文件元数据打包成一个 CommitIncrement 对象（最终会被上层的 Committer 用来生成 Manifest 文件和 Snapshot）。同时清空这些集合，为下一个 Checkpoint 做准备
        return drainIncrement();
    }

    @Override
    public boolean compactNotCompleted() {
        compactManager.triggerCompaction(false);
        return compactManager.compactNotCompleted();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    private CommitIncrement drainIncrement() {
        // 新写入、changelog文件，删除的文件，打包到DataIncrement
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));
        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();
        compactDeletionFile = null;

        // DataIncrement -> CommitIncrement，完整地描述了一次提交的所有文件变化（新增文件、合并前后文件等）。Paimon 的提交进程会把这个 CommitIncrement 的内容记录到 Manifest 文件中，从而生成一个新的快照（Snapshot）
        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }

    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    /**
     * 用于处理压缩结果的核心方法，它在每次压缩操作完成后被调用，用于更新文件状态和清理不再需要的文件
     */
    private void updateCompactResult(CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file);
                }
            } else {
                // 将被合并掉的老文件加入 compactBefore 集合
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after()); // 将合并后产生的新文件加入 compactAfter 集合
        compactChangelog.addAll(result.changelog());

        updateCompactDeletionFile(result.deletionFile());
    }

    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file);
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file);
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file);
        }

        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }
}
