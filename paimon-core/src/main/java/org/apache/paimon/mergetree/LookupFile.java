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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lookup file for cache remote file to local.
 * 由LookupLevels#createLookupFile创建，LookupFile 对象本身并不直接在其内存中存储具体的数据。它更像是一个描述符或句柄，管理着一个本地磁盘文件，并提供了访问这个本地文件内容的方法
 * LookupFile负责管理 localFile 的生命周期（创建、缓存、删除）和元数据（如对应的远程文件）
 * LookupStoreReader 则封装了从特定格式的 localFile 中读取/查找键值对的逻辑。LookupFile 将具体的读操作委托给 LookupStoreReader
 * 当需要对某个 DataFileMeta 所描述的远程文件进行点查时，系统首先会检查本地是否存在一个对应的、已经构建好的LookupFile
 * 如果存在（缓存命中），则直接使用这个本地查找文件进行快速查询，避免了读取和解析庞大的远程列式文件
 * 本地文件缓存 (Local File Cache): Paimon 会将远程存储（如 HDFS/S3）上的数据文件拉取到本地磁盘进行缓存，避免每次查询都通过网络读取并解析。这个缓存由 Caffeine 实现，具备 LRU 和超时淘汰策略。
 * 本地文件的具体格式和内容（例如，是完整的数据拷贝、仅包含键的索引、还是带有布隆过滤器等）由 LookupStoreFactory 及其具体的实现（如 SortLookupStoreFactory 或 HashLookupStoreFactory）决定。
 * */
public class LookupFile {

    private static final Logger LOG = LoggerFactory.getLogger(LookupFile.class);

    private final File localFile; // lookup时在本地建的一个优化过的副本或索引，加速后续查询
    private final DataFileMeta remoteFile; // 存储了对应的原始远程数据文件的元信息，如文件名、大小、行数等
    private final LookupStoreReader reader; // 用于从 localFile 中根据键查找对应的值。具体的实现由 LookupStoreFactory 提供
    private final Runnable callback; // 用于在文件被从缓存中移除并关闭后执行一些清理操作（例如从 LookupLevels 的 ownCachedFiles 集合中移除记录）

    private long requestCount; // 统计此本地缓存文件的访问情况
    private long hitCount; // 统计此本地缓存文件的命中情况
    private boolean isClosed = false;

    public LookupFile(
            File localFile, DataFileMeta remoteFile, LookupStoreReader reader, Runnable callback) {
        this.localFile = localFile;
        this.remoteFile = remoteFile;
        this.reader = reader;
        this.callback = callback;
    }

    @Nullable
    public byte[] get(byte[] key) throws IOException {
        checkArgument(!isClosed);
        requestCount++;
        byte[] res = reader.lookup(key); // 实际调用的是reader#lookup
        if (res != null) {
            hitCount++;
        }
        return res;
    }

    public DataFileMeta remoteFile() {
        return remoteFile;
    }

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * 当这个 LookupFile 从缓存中被移除时调用。
     * 1.关闭底层的 reader，执行一个回调函数（callback，通常用于从 LookupLevels 中移除对这个文件的跟踪）
     * 2.记录相关的日志信息（包括移除原因、访问统计、文件大小）
     * 3.最终删除本地磁盘上的 localFile
     */
    public void close(RemovalCause cause) throws IOException {
        reader.close();
        isClosed = true;
        callback.run();
        LOG.info(
                "Delete Lookup file {} due to {}. Access stats: requestCount={}, hitCount={}, size={}KB",
                localFile.getName(),
                cause,
                requestCount,
                hitCount,
                localFile.length() >> 10);
        FileIOUtils.deleteFileOrDirectory(localFile);
    }

    // ==================== Cache for Local File ======================

    /**
     * 配置了缓存的过期策略（基于访问时间 fileRetention）、
     * 最大权重（基于磁盘大小 maxDiskSize）、
     * 权重计算方式（本地文件大小 fileWeigh）、
     * 移除监听器（removalCallback，即调用 LookupFile.close()）
     */
    public static Cache<String, LookupFile> createCache(
            Duration fileRetention, MemorySize maxDiskSize) {
        return Caffeine.newBuilder()
                .expireAfterAccess(fileRetention)
                .maximumWeight(maxDiskSize.getKibiBytes())
                .weigher(LookupFile::fileWeigh)
                .removalListener(LookupFile::removalCallback)
                .executor(Runnable::run)
                .build();
    }

    private static int fileWeigh(String file, LookupFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    private static void removalCallback(String file, LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            try {
                lookupFile.close(cause);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 生成本地缓存文件的唯一前缀，通常会包含分区信息、bucket ID 和原始远程文件名，以避免冲突
     */
    public static String localFilePrefix(
            RowType partitionType, BinaryRow partition, int bucket, String remoteFileName) {
        if (partition.getFieldCount() == 0) {
            return String.format("%s-%s", bucket, remoteFileName);
        } else {
            String partitionString = partToSimpleString(partitionType, partition, "-", 20);
            return String.format("%s-%s-%s", partitionString, bucket, remoteFileName);
        }
    }
}
