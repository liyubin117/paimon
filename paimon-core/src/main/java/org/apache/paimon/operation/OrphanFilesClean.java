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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SupplierWithIOException;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.ChangelogManager.CHANGELOG_PREFIX;
import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.HintFileUtils.EARLIEST;
import static org.apache.paimon.utils.HintFileUtils.LATEST;
import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/**
 * To remove the data files and metadata files that are not used by table (so-called "orphan
 * files").
 * 设计目标：
 * 清理未被表引用的"孤儿文件"，释放存储空间
 * 避免误删正在写入的新文件（通过时间阈值控制）
 * 处理并发场景下的文件冲突问题
 * 支持本地和分布式两种清理模式

 *
     OrphanFilesClean (抽象类，抽象基类定义核心算法)
     ↓
     LocalOrphanFilesClean (本地实现，使用线程池)
     ↓
     FlinkOrphanFilesClean / SparkOrphanFilesClean (分布式实现)

 *
 * <p>It will ignore exception when listing all files because it's OK to not delete unread files.
 *
 * <p>To avoid deleting newly written files, it only deletes orphan files older than {@code
 * olderThanMillis} (1 day by default).
 *
 * <p>To avoid conflicting with snapshot expiration, tag deletion and rollback, it will skip the
 * snapshot/tag when catching {@link FileNotFoundException} in the process of listing used files.
 *
 * <p>To avoid deleting files that are used but not read by mistaken, it will stop removing process
 * when failed to read used files.
 */
public abstract class OrphanFilesClean implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(OrphanFilesClean.class);

    protected static final int READ_FILE_RETRY_NUM = 3;
    protected static final int READ_FILE_RETRY_INTERVAL = 5;

    protected final FileStoreTable table;
    protected final FileIO fileIO;
    protected final long olderThanMillis;
    protected final boolean dryRun;
    protected final int partitionKeysNum;
    protected final Path location;

    public OrphanFilesClean(FileStoreTable table, long olderThanMillis, boolean dryRun) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.partitionKeysNum = table.partitionKeys().size();
        this.location = table.location();
        this.olderThanMillis = olderThanMillis;
        this.dryRun = dryRun;
    }

    protected List<String> validBranches() {
        List<String> branches = table.branchManager().branches();
        branches.add(DEFAULT_MAIN_BRANCH);

        // 检查异常分支（没有schema的分支）
        List<String> abnormalBranches = new ArrayList<>();
        for (String branch : branches) {
            SchemaManager schemaManager = table.schemaManager().copyWithBranch(branch);
            if (!schemaManager.latest().isPresent()) {
                abnormalBranches.add(branch);
            }
        }
        if (!abnormalBranches.isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Branches %s have no schemas. Orphan files cleaning aborted. "
                                    + "Please check these branches manually.",
                            abnormalBranches));
        }
        return branches;
    }

    protected void cleanSnapshotDir(
            List<String> branches,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        for (String branch : branches) {
            cleanBranchSnapshotDir(branch, deletedFilesConsumer, deletedFilesLenInBytesConsumer);
        }
    }

    protected void cleanBranchSnapshotDir(
            String branch,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        LOG.info("Start to clean snapshot directory of branch {}.", branch);
        FileStoreTable branchTable = table.switchToBranch(branch);
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        ChangelogManager changelogManager = branchTable.changelogManager();

        // specially handle the snapshot directory
        List<Pair<Path, Long>> nonSnapshotFiles =
                tryGetNonSnapshotFiles(snapshotManager.snapshotDirectory(), this::oldEnough);
        nonSnapshotFiles.forEach(
                nonSnapshotFile ->
                        cleanFile(
                                nonSnapshotFile,
                                deletedFilesConsumer,
                                deletedFilesLenInBytesConsumer));

        // specially handle the changelog directory
        List<Pair<Path, Long>> nonChangelogFiles =
                tryGetNonChangelogFiles(changelogManager.changelogDirectory(), this::oldEnough);
        nonChangelogFiles.forEach(
                nonChangelogFile ->
                        cleanFile(
                                nonChangelogFile,
                                deletedFilesConsumer,
                                deletedFilesLenInBytesConsumer));
        LOG.info("End to clean snapshot directory of branch {}.", branch);
    }

    private List<Pair<Path, Long>> tryGetNonSnapshotFiles(
            Path snapshotDirectory, Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(snapshotDirectory, fileStatusFilter, nonSnapshotFileFilter());
    }

    private List<Pair<Path, Long>> tryGetNonChangelogFiles(
            Path changelogDirectory, Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(changelogDirectory, fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Pair<Path, Long>> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        List<FileStatus> statuses = tryBestListingDirs(directory);
        return statuses.stream()
                .filter(fileStatusFilter)
                .filter(status -> fileFilter.test(status.getPath()))
                .map(status -> Pair.of(status.getPath(), status.getLen()))
                .collect(Collectors.toList());
    }

    private static Predicate<Path> nonSnapshotFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private static Predicate<Path> nonChangelogFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private void cleanFile(
            Pair<Path, Long> deleteFileInfo,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        Path filePath = deleteFileInfo.getLeft();
        Long fileSize = deleteFileInfo.getRight();
        deletedFilesConsumer.accept(filePath);
        deletedFilesLenInBytesConsumer.accept(fileSize);
        cleanFile(filePath);
    }

    /**
     * 文件删除策略与错误处理
     *  使用 deleteQuietly 方法，删除失败时不抛出异常
     *  避免因为个别文件删除失败而中断整个清理流程
     *  通过 dryRun 模式支持预演，不实际删除文件
     */
    protected void cleanFile(Path path) {
        if (!dryRun) {
            try {
                if (fileIO.isDir(path)) {
                    fileIO.deleteDirectoryQuietly(path); // 静默删除目录
                } else {
                    fileIO.deleteQuietly(path); // 静默删除文件
                }
            } catch (IOException ignored) { // 忽略删除异常，避免阻塞清理流程
            }
        }
    }

    protected Set<Snapshot> safelyGetAllSnapshots(String branch) throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        ChangelogManager changelogManager = branchTable.changelogManager();
        TagManager tagManager = branchTable.tagManager();
        Set<Snapshot> readSnapshots = new HashSet<>(snapshotManager.safelyGetAllSnapshots());
        readSnapshots.addAll(tagManager.taggedSnapshots());
        readSnapshots.addAll(changelogManager.safelyGetAllChangelogs());
        return readSnapshots;
    }

    protected void collectWithoutDataFile(
            String branch,
            Snapshot snapshot,
            Consumer<String> usedFileConsumer,
            Consumer<String> manifestConsumer)
            throws IOException {
        Consumer<Pair<String, Boolean>> usedFileWithFlagConsumer =
                fileAndFlag -> {
                    if (fileAndFlag.getRight()) {
                        manifestConsumer.accept(fileAndFlag.getLeft());
                    }
                    usedFileConsumer.accept(fileAndFlag.getLeft());
                };
        collectWithoutDataFileWithManifestFlag(branch, snapshot, usedFileWithFlagConsumer);
    }

    protected void collectWithoutDataFileWithManifestFlag(
            String branch,
            Snapshot snapshot,
            Consumer<Pair<String, Boolean>> usedFileWithFlagConsumer)
            throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        IndexFileHandler indexFileHandler = branchTable.store().newIndexFileHandler();
        List<ManifestFileMeta> manifestFileMetas = new ArrayList<>();
        // changelog manifest
        if (snapshot.changelogManifestList() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.changelogManifestList(), false));
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () ->
                                    manifestList.readWithIOException(
                                            snapshot.changelogManifestList()),
                            emptyList()));
        }

        // delta manifest
        if (snapshot.deltaManifestList() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.deltaManifestList(), false));
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () -> manifestList.readWithIOException(snapshot.deltaManifestList()),
                            emptyList()));
        }

        // base manifest
        usedFileWithFlagConsumer.accept(Pair.of(snapshot.baseManifestList(), false));
        manifestFileMetas.addAll(
                retryReadingFiles(
                        () -> manifestList.readWithIOException(snapshot.baseManifestList()),
                        emptyList()));

        // collect manifests
        for (ManifestFileMeta manifest : manifestFileMetas) {
            usedFileWithFlagConsumer.accept(Pair.of(manifest.fileName(), true));
        }

        // index files
        String indexManifest = snapshot.indexManifest();
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            usedFileWithFlagConsumer.accept(Pair.of(indexManifest, false));
            retryReadingFiles(
                            () -> indexFileHandler.readManifestWithIOException(indexManifest),
                            Collections.<IndexManifestEntry>emptyList())
                    .stream()
                    .map(IndexManifestEntry::indexFile)
                    .map(IndexFileMeta::fileName)
                    .forEach(name -> usedFileWithFlagConsumer.accept(Pair.of(name, false)));
        }

        // statistic file
        if (snapshot.statistics() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.statistics(), false));
        }
    }

    /** List directories that contains data files and manifest files. */
    protected List<Path> listPaimonFileDirs() {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        return listPaimonFileDirs(
                table.fullName(),
                pathFactory.manifestPath().toString(),
                pathFactory.indexPath().toString(),
                pathFactory.statisticsPath().toString(),
                pathFactory.dataFilePath().toString(),
                partitionKeysNum,
                table.store().options().dataFileExternalPaths());
    }

    protected List<Path> listPaimonFileDirs(
            String tableName,
            String manifestPath,
            String indexPath,
            String statisticsPath,
            String dataFilePath,
            int partitionKeysNum,
            String dataFileExternalPaths) {
        LOG.info("Start: listing paimon file directories for table [{}]", tableName);
        long start = System.currentTimeMillis();
        List<Path> paimonFileDirs = new ArrayList<>();

        paimonFileDirs.add(new Path(manifestPath));
        paimonFileDirs.add(new Path(indexPath));
        paimonFileDirs.add(new Path(statisticsPath));
        paimonFileDirs.addAll(listFileDirs(new Path(dataFilePath), partitionKeysNum));

        // add external data paths
        if (dataFileExternalPaths != null) {
            String[] externalPathArr = dataFileExternalPaths.split(",");
            for (String externalPath : externalPathArr) {
                paimonFileDirs.addAll(listFileDirs(new Path(externalPath), partitionKeysNum));
            }
        }
        LOG.info(
                "End list paimon file directories for table [{}] spend [{}] ms",
                tableName,
                System.currentTimeMillis() - start);
        return paimonFileDirs;
    }

    /**
     * List directories that contains data files. The argument level is used to control recursive
     * depth.
     */
    private List<Path> listFileDirs(Path dir, int level) {
        List<FileStatus> dirs = tryBestListingDirs(dir);

        if (level == 0) {
            // return bucket paths
            return filterDirs(dirs, p -> p.getName().startsWith(BUCKET_PATH_PREFIX));
        }

        List<Path> partitionPaths = filterDirs(dirs, p -> p.getName().contains("="));

        List<Path> result = new ArrayList<>();
        for (Path partitionPath : partitionPaths) {
            result.addAll(listFileDirs(partitionPath, level - 1));
        }
        return result;
    }

    private List<Path> filterDirs(List<FileStatus> statuses, Predicate<Path> filter) {
        List<Path> filtered = new ArrayList<>();

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (filter.test(path)) {
                filtered.add(path);
            }
            // ignore unknown dirs
        }

        return filtered;
    }

    /**
     * If failed to list directory, just return an empty result because it's OK to not delete them.
     */
    protected List<FileStatus> tryBestListingDirs(Path dir) {
        try {
            if (!fileIO.exists(dir)) {
                return emptyList();
            }

            return retryReadingFiles(
                    () -> {
                        FileStatus[] s = fileIO.listStatus(dir);
                        return s == null ? emptyList() : Arrays.asList(s);
                    },
                    emptyList());
        } catch (IOException e) {
            LOG.debug("Failed to list directory {}, skip it.", dir, e);
            return emptyList();
        }
    }

    /**
     * 文件读取重试机制，区别对待FileNotFoundException 和其他 IOException
     *  文件不存在时返回默认值，避免阻塞清理流程
     *  其他IO异常进行重试，提高容错性
     * Retry reading files when {@link IOException} was thrown by the reader. If the exception is
     * {@link FileNotFoundException}, return default value. Finally, if retry times reaches the
     * limits, rethrow the IOException.
     */
    protected static <T> T retryReadingFiles(SupplierWithIOException<T> reader, T defaultValue)
            throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) { // 最多重试3次
            try {
                return reader.get();
            } catch (FileNotFoundException e) {
                return defaultValue; // 文件不存在返回默认值
            } catch (IOException e) {
                caught = e; // 记录异常继续重试
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_FILE_RETRY_INTERVAL); // 间隔100ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        throw caught; // 超过重试次数抛出异常
    }

    /**
     * 时间窗口保护机制：
     *  默认只清理1天前的文件，避免误删正在写入的文件
     *  支持用户自定义时间阈值
     *  强制校验时间必须早于当前时间
     */
    protected boolean oldEnough(FileStatus status) {
        return status.getModificationTime() < olderThanMillis;
    }

    public static long olderThanMillis(@Nullable String olderThan) {
        if (isNullOrWhitespaceOnly(olderThan)) {
            return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        } else {
            // 解析用户指定的时间戳
            Timestamp parsedTimestampData =
                    DateTimeUtils.parseTimestampData(olderThan, 3, TimeZone.getDefault());
            Preconditions.checkArgument(
                    parsedTimestampData.compareTo(
                                    Timestamp.fromEpochMillis(System.currentTimeMillis()))
                            < 0,
                    "The arg olderThan must be less than now, because dataFiles that are currently being written and not referenced by snapshots will be mistakenly cleaned up.");

            return parsedTimestampData.getMillisecond();
        }
    }

    /** Try to clean empty data directories. */
    protected void tryCleanDataDirectory(Set<Path> dataDirs, int maxLevel) {
        for (int level = 0; level < maxLevel; level++) {
            dataDirs =
                    dataDirs.stream()
                            .filter(this::tryDeleteEmptyDirectory)
                            .map(Path::getParent)
                            .collect(Collectors.toSet());
        }
    }

    public boolean tryDeleteEmptyDirectory(Path path) {
        if (dryRun) {
            return false;
        }

        try {
            return fileIO.delete(path, false);
        } catch (IOException e) {
            return false;
        }
    }

    /** Cleaner to clean files. */
    public interface FileCleaner extends Serializable {

        void clean(String table, Path path);
    }
}
