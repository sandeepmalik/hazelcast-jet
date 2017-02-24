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

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Util.entry;

/**
 * Javadoc pending.
 */
public class GroupByFrameP<T, B, F, R> extends AbstractProcessor {
    private final TwoTieredCollector<T, B, F, R> tc;
    private final ToLongFunction<? super T> extractTimestampF;
    private final LongUnaryOperator toFrameSeqF;
    private final int bucketCount;
    private final B[] buckets;
    private long currentFrameSeq;

    public GroupByFrameP(int bucketCount,
                         ToLongFunction<? super T> extractTimestampF,
                         LongUnaryOperator toFrameSeqF,
                         TwoTieredCollector<T, B, F, R> tc
    ) {
        this.tc = tc;
        this.extractTimestampF = extractTimestampF;
        this.toFrameSeqF = toFrameSeqF;
        this.bucketCount = bucketCount;
        this.buckets = (B[]) new Object[bucketCount];
        Arrays.setAll(buckets, i -> tc.bucketSupplier().get());
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        final long itemFrameSeq = toFrameSeqF.applyAsLong(extractTimestampF.applyAsLong(t));
        ensureFrameSeqInitialized(itemFrameSeq);
        if (itemFrameSeq <= currentFrameSeq - bucketCount) {
            System.out.println("Late event: " + t);
            return true;
        }
        if (itemFrameSeq > currentFrameSeq) {
            slideTo(itemFrameSeq);
        }
        tc.accumulator().accept(buckets[toBucketIndex(itemFrameSeq)], t);
        return true;
    }

    private void slideTo(long itemFrameSeq) {
        final long bucketCountToEvict = itemFrameSeq - currentFrameSeq;
        final long oldestFrameSeq = currentFrameSeq - bucketCount + 1;
        for (long i = 0; i < bucketCountToEvict; i++) {
            final long frameSeqBeingEvicted = oldestFrameSeq + i;
            final int bucketIndex = toBucketIndex(frameSeqBeingEvicted);
            emit(entry(frameSeqBeingEvicted, tc.bucketToFrameTransformer().apply(buckets[bucketIndex])));
            buckets[bucketIndex] = tc.bucketSupplier().get();
        }
        currentFrameSeq = itemFrameSeq;
    }

    private int toBucketIndex(long tsPeriod) {
        return (int) (tsPeriod % bucketCount);
    }

    private void ensureFrameSeqInitialized(long frameSeq) {
        if (currentFrameSeq == 0) {
            currentFrameSeq = frameSeq;
        }
    }
}
