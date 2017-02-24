/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.windowing.example;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

/**
 * TODO javadoc
 * @param <T> Ingested type
 * @param <B> Accumulator type
 * @param <R> Accumulator result type
 */
public class GroupByFrameP<T, B, R> extends AbstractProcessor {
    private final SnapshottingCollector<T, B, R> tc;
    private final ToLongFunction<? super T> extractTimestampF;
    private final LongUnaryOperator toFrameSeqF;
    private final int bucketCount;
    private final Map<Object, B[]> bucketsForKey;
    private final Function<? super T, ?> keyExtractor;
    private long currentFrameSeq;
    private long frameSeqBase;

    public GroupByFrameP(int bucketCount,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, B, R> tc
    ) {
        this(bucketCount, extractTimestampF, toFrameSeqF, tc, t -> true);
    }

    public GroupByFrameP(int bucketCount,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, B, R> tc,
            Function<? super T, ?> keyExtractor
    ) {
        this.tc = tc;
        this.extractTimestampF = extractTimestampF;
        this.toFrameSeqF = toFrameSeqF;
        this.bucketCount = bucketCount;
        this.bucketsForKey = new HashMap<>();
        this.keyExtractor = keyExtractor;
    }

    private B[] newBuckets(Object key) {
        B[] res = (B[]) new Object[bucketCount];
        Arrays.setAll(res, i -> tc.supplier().get());
        return res;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        Object key = keyExtractor.apply(t);
        final long itemFrameSeq = toFrameSeqF.applyAsLong(extractTimestampF.applyAsLong(t));
        ensureFrameSeqInitialized(itemFrameSeq);
        if (itemFrameSeq <= currentFrameSeq - bucketCount) {
            System.out.println("Late event: " + t);
            return true;
        }
        if (itemFrameSeq > currentFrameSeq) {
            slideTo(itemFrameSeq);
        }
        B[] buckets = bucketsForKey.computeIfAbsent(key, this::newBuckets);
        tc.accumulator().accept(buckets[toBucketIndex(itemFrameSeq)], t);
        return true;
    }

    private void slideTo(long itemFrameSeq) {
        final long evictFrom = Math.max(frameSeqBase, currentFrameSeq - bucketCount + 1);
        final long evictUntil = itemFrameSeq - bucketCount + 1;
        for (long seq = evictFrom; seq < evictUntil; seq++) {
            final int bucketIndex = toBucketIndex(seq);
            for (Entry<Object, B[]> entry : bucketsForKey.entrySet()) {
                B[] buckets = entry.getValue();
                emit(new KeyedWindowEntry<>(seq, entry.getKey(), buckets[bucketIndex]));
                buckets[bucketIndex] = tc.supplier().get();
            }
        }
        currentFrameSeq = itemFrameSeq;
    }

    private int toBucketIndex(long tsPeriod) {
        return (int) Math.floorMod(tsPeriod, bucketCount);
    }

    private void ensureFrameSeqInitialized(long frameSeq) {
        if (currentFrameSeq == 0) {
            currentFrameSeq = frameSeq;
            frameSeqBase = frameSeq;
        }
    }

    public static final class KeyedWindowEntry<K, V> {
        private final long seq;
        private final K key;
        private final V value;

        public KeyedWindowEntry(long seq, K key, V value) {
            this.seq = seq;
            this.key = key;
            this.value = value;
        }

        public long getSeq() {
            return seq;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "KeyedWindowEntry{" +
                    "seq=" + seq +
                    ", key=" + key +
                    ", value=" + value +
                    '}';
        }
    }
}
