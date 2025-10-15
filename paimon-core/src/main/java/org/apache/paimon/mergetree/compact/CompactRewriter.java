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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.Closeable;
import java.util.List;

/** Rewrite sections to new level.
 * 读取多个旧文件，合并它们，然后生成新的、更优化的文件。它是一个“重写者”
 * 如果开启了dv，在合并过程中，它有机会发现“某个在高层级文件中的数据需要被标记为删除”，这时就需要更新索引
 * */
public interface CompactRewriter extends Closeable {

    /**
     * Rewrite sections to new level.
     *
     * @param outputLevel new level
     * @param dropDelete whether to drop the deletion, see {@link
     *     MergeTreeCompactManager#triggerCompaction}
     * @param sections list of sections (section is a list of {@link SortedRun}s, and key intervals
     *     between sections do not overlap)
     * @return compaction result
     * @throws Exception exception
     */
    CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception;

    /**
     * Upgrade file to new level, usually file data is not rewritten, only the metadata is updated.
     * But in some certain scenarios, we must rewrite file too, e.g. {@link
     * ChangelogMergeTreeRewriter}
     *
     * @param outputLevel new level
     * @param file file to be updated
     * @return compaction result
     * @throws Exception exception
     */
    CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception;
}
