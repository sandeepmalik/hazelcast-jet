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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * TODO javadoc
 * @param <T> Ingested type
 * @param <F> Accumulator type
 * @param <R> Accumulator result type
 */
public class GroupByFrameP<T, K, F, R> extends AbstractProcessor {
    private final SnapshottingCollector<? super T, F, R> tc;
    private final ToLongFunction<? super T> extractTimestampF;
    private final Function<? super T, K> extractKeyF;
    private final LongUnaryOperator toFrameSeqF;
    private final int openFrameCount;
    private final Map<K, F>[] keyToFrameMaps;
    private long currentFrameSeq;
    private long frameSeqBase;

    private GroupByFrameP(
            int openFrameCount,
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<? super T, F, R> tc
    ) {
        this.tc = tc;
        this.extractTimestampF = extractTimestampF;
        this.extractKeyF = extractKeyF;
        this.toFrameSeqF = toFrameSeqF;
        this.openFrameCount = openFrameCount;
        this.keyToFrameMaps = new Map[openFrameCount];
        Arrays.setAll(keyToFrameMaps, i -> new HashMap<>());
    }

    public static <T, B, R> Distributed.Supplier<GroupByFrameP> groupByFrame(
            int openFrameCount,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, B, R> tc) {
        return groupByFrame(openFrameCount, t -> "global", extractTimestampF, toFrameSeqF, tc);
    }

    public static <T, K, B, R> Distributed.Supplier<GroupByFrameP> groupByFrame(
            int openFrameCount,
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, B, R> tc) {
        return () -> new GroupByFrameP<>(openFrameCount, extractKeyF, extractTimestampF, toFrameSeqF, tc);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        K key = extractKeyF.apply(t);
        final long itemFrameSeq = toFrameSeqF.applyAsLong(extractTimestampF.applyAsLong(t));
        ensureFrameSeqInitialized(itemFrameSeq);
        if (itemFrameSeq <= currentFrameSeq - openFrameCount) {
            System.out.println("Late event: " + t);
            return true;
        }
        if (itemFrameSeq > currentFrameSeq) {
            slideTo(itemFrameSeq);
        }
        F frame = keyToFrameMaps[toFrameIndex(itemFrameSeq)].computeIfAbsent(key, x -> tc.supplier().get());
        tc.accumulator().accept(frame, t);
        return true;
    }

    private void slideTo(long itemFrameSeq) {
        final long evictFrom = Math.max(frameSeqBase, currentFrameSeq - openFrameCount + 1);
        final long evictUntil = itemFrameSeq - openFrameCount + 1;
        for (long seq = evictFrom; seq < evictUntil; seq++) {
            final int frameIndex = toFrameIndex(seq);
            for (Entry<K, F> e : keyToFrameMaps[frameIndex].entrySet()) {
                emit(new KeyedFrame<>(seq, e.getKey(), e.getValue()));
            }
            emit(new Punctuation(seq));
            keyToFrameMaps[frameIndex] = new HashMap<>();
        }
        currentFrameSeq = itemFrameSeq;
    }

    private int toFrameIndex(long tsPeriod) {
        return (int) Math.floorMod(tsPeriod, openFrameCount);
    }

    private void ensureFrameSeqInitialized(long frameSeq) {
        if (currentFrameSeq == 0) {
            currentFrameSeq = frameSeq;
            frameSeqBase = frameSeq;
        }
    }
}
