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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Read splits from specified {@link Snapshot} with given configuration. */
public interface SnapshotReader {

    @Nullable
    Integer parallelism();

    SnapshotManager snapshotManager();

    ChangelogManager changelogManager();

    ManifestsReader manifestsReader();

    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    ConsumerManager consumerManager();

    SplitGenerator splitGenerator();

    FileStorePathFactory pathFactory();

    SnapshotReader withSnapshot(long snapshotId);

    SnapshotReader withSnapshot(Snapshot snapshot);

    SnapshotReader withFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec);

    SnapshotReader withPartitionFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(List<BinaryRow> partitions);

    SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate);

    SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions);

    SnapshotReader withMode(ScanMode scanMode);

    SnapshotReader withLevel(int level);

    SnapshotReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotReader enableValueFilter();

    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter);

    SnapshotReader withBucket(int bucket);

    SnapshotReader onlyReadRealBuckets();

    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter);

    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter);

    SnapshotReader dropStats();

    SnapshotReader keepStats();

    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    SnapshotReader withMetricRegistry(MetricRegistry registry);

    /** Get splits plan from snapshot. */
    Plan read();

    /** Get splits plan from file changes. */
    Plan readChanges();

    Plan readIncrementalDiff(Snapshot before);

    /** List partitions. */
    List<BinaryRow> partitions();

    List<PartitionEntry> partitionEntries();

    List<BucketEntry> bucketEntries();

    Iterator<ManifestEntry> readFileIterator();

    /** Result plan of this scan.
     * 是一份数据读取的执行计划，而不是数据本身
     * */
    interface Plan extends TableScan.Plan {

        /**
         * 这个快照所携带的水印信息，用于事件时间处理
         */
        @Nullable
        Long watermark();

        /**
         * Snapshot id of this plan, return null if the table is empty or the manifest list is
         * specified.
         * 这个 Plan 是基于哪个快照版本生成的
         */
        @Nullable
        Long snapshotId();

        /**
         * Splits (数据切分): 这是 Plan 最核心的内容。它是一个 Split 对象的列表，在 Paimon 中通常是 DataSplit。
         *
         * 每一个 DataSplit 代表一个独立的、可以被单个并发任务处理的工作单元。
         * DataSplit 内部详细定义了：
         *      分区信息 (Partition): 这批数据属于哪个分区。
         *      桶ID (Bucket): 数据在哪个桶里。
         *      数据文件列表 (Data Files): 具体要读取的一个或多个数据文件（如 Parquet/ORC 文件）的路径和元信息
         */
        List<Split> splits();

        @SuppressWarnings({"unchecked", "rawtypes"})
        default List<DataSplit> dataSplits() {
            return (List) splits();
        }
    }
}
