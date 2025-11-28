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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

/**
 * {@link TableScan} for streaming, supports {@link #checkpoint} and {@link #restore}.
 * 流式读取核心接口
 * 定义了流式作业所必需的关键能力，比如从某个状态点 (restore) 开始消费、在 Checkpoint 时保存当前消费进度 (checkpoint)、以及在 Checkpoint 完成后通知 (notifyCheckpointComplete) 等
 *
 * <p>NOTE: {@link #checkpoint} will return the next snapshot id.
 *
 * @since 0.4.0
 */
@Public
public interface StreamTableScan extends TableScan, Restorable<Long> {

    /** Current watermark for consumed snapshot. */
    @Nullable
    Long watermark();

    /** Restore from checkpoint next snapshot id.
     * 当作业从失败中恢复时，Flink 会从状态后端读取之前保存的 nextSnapshotId，并通过此方法设置回来。这样，DataTableStreamScan 就知道应该从哪个快照开始继续消费，避免了数据丢失或重复
     * */
    @Override
    void restore(@Nullable Long nextSnapshotId);

    /** Checkpoint to return next snapshot id.
     * 当 Flink 等引擎触发 checkpoint 时，会调用此方法。它直接返回当前已经处理完毕、下一个待处理的 nextSnapshotId。这个 ID 会被 Flink 保存到状态后端
     * */
    @Nullable
    @Override
    Long checkpoint();

    /** Notifies the checkpoint complete with next snapshot id.
     * 当一个 checkpoint 成功完成后，此方法被调用。如果配置了 consumer-id，它会更新 Paimon 表中记录的消费位点（consumer），这对于监控和跨作业/跨集群的消费进度管理非常有用
     * */
    void notifyCheckpointComplete(@Nullable Long nextSnapshot);
}
