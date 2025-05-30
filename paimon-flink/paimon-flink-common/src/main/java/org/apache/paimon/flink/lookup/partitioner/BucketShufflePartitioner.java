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

package org.apache.paimon.flink.lookup.partitioner;

import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle.InputDataPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.MathUtils;

/**
 * {@link BucketShufflePartitioner} class partitions rows based on the bucket id. It uses a custom
 * strategy and an extractor to determine the target partition for a given set of join keys.
 */
public class BucketShufflePartitioner implements InputDataPartitioner {

    private final ShuffleStrategy strategy;

    private final BucketIdExtractor extractor;

    public BucketShufflePartitioner(ShuffleStrategy strategy, BucketIdExtractor extractor) {
        this.strategy = strategy;
        this.extractor = extractor;
    }

    @Override
    public int partition(RowData joinKeys, int numPartitions) {
        int bucketId = extractor.extractBucketId(joinKeys);
        int joinKeyHash = MathUtils.murmurHash(joinKeys.hashCode());
        return strategy.getTargetSubtaskId(bucketId, joinKeyHash, numPartitions);
    }
}
