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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.Filter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** A lookup table which provides get and refresh.
 * 定义了维表初始化、查找和缓存管理的基本操作
 * */
public interface LookupTable extends Closeable {

    // 为scan.partitions场景设置分区过滤器
    void specificPartitionFilter(Predicate filter);

    // 加载初始缓存
    void open() throws Exception;

    // 根据指定的key查找匹配的所有行
    List<InternalRow> get(InternalRow key) throws IOException;

    // 刷新缓存
    void refresh() throws Exception;

    // 设置缓存的行过滤器
    void specifyCacheRowFilter(Filter<InternalRow> filter);
}
