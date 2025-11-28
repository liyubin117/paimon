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

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.List;

/** Helper class for the first planning of {@link TableScan}. */
public interface StartingScanner {

    StartingContext startingContext();

    /**
     * 返回结果有三种可能：
     * ScannedResult: 成功扫描到了一个起始快照，并生成了 Plan。此时，方法会更新 nextSnapshotId 为当前快照 ID + 1，并返回这个 Plan。
     * NextSnapshot: 表里虽然有数据，但根据启动策略（比如从一个未来的时间戳启动），当前不需要读取任何数据。它会告诉 DataTableStreamScan 应该从哪个快照 ID (nextSnapshotId) 开始等。
     * NoSnapshot: 表里没有任何快照，什么也不做，返回一个空计划 SnapshotNotExistPlan.INSTANCE
     */
    Result scan(SnapshotReader snapshotReader);

    List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader);

    /** Scan result of {@link #scan}. */
    interface Result {}

    /** Currently, there is no snapshot, need to wait for the snapshot to be generated. */
    class NoSnapshot implements Result {}

    static ScannedResult fromPlan(SnapshotReader.Plan plan) {
        return new ScannedResult(plan);
    }

    /** Result with scanned snapshot. Next snapshot should be the current snapshot plus 1. */
    class ScannedResult implements Result {

        private final SnapshotReader.Plan plan;

        public ScannedResult(SnapshotReader.Plan plan) {
            this.plan = plan;
        }

        public long currentSnapshotId() {
            return plan.snapshotId();
        }

        @Nullable
        public Long currentWatermark() {
            return plan.watermark();
        }

        public List<DataSplit> splits() {
            return (List) plan.splits();
        }

        public SnapshotReader.Plan plan() {
            return plan;
        }
    }

    /**
     * Return the next snapshot for followup scanning. The current snapshot is not scanned (even
     * doesn't exist), so there are no splits.
     * 因为流作业的启动时间点，和它想要开始处理数据的时间点，可能是不同的，如果出现这种情况，应返回下一次从哪个快照开始读，而不是异常或直接读
     * 是一种“延迟启动”的信号。它告诉流处理引擎：“现在还没到你该干活的时候，但别退出，请从快照ID X 开始等着，有新数据了我会告诉你。” 这使得 Paimon 能够精确地从用户指定的、哪怕是未来的某个时间点或快照开始消费，而不会丢失数据
     */
    class NextSnapshot implements Result {

        private final long nextSnapshotId;

        public NextSnapshot(long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
        }

        public long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
