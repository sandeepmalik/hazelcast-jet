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

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.Distributed.Function.identity;

/**
 * Groups items into frames. A frame is identified by its {@code long frameSeq};
 * the {@code extractFrameSeqF} function determines this number for each item.
 * Within a frame items are further classified by a grouping key determined by
 * the {@code extractKeyF} function. When the processor receives a punctuation
 * with a given {@code puncSeq}, it emits the current state of all frames with
 * {@code frameSeq <= puncSeq} and deletes these frames from its storage.
 *
 * @param <T> item type
 * @param <K> type of key returned from {@code extractKeyF}
 * @param <F> type of frame returned from {@code sc.supplier()}
 */
public class GroupByFrameP<T, K, F> extends FrameProcessorBase<K, F, F> {
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final LongUnaryOperator toFrameSeqF;
    private final Supplier<F> supplier;
    private final BiConsumer<F, ? super T> accumulator;

    private GroupByFrameP(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractEventSeqF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<? super T, F, ?> collector
    ) {
        super(identity());
        this.extractKeyF = extractKeyF;
        this.extractEventSeqF = extractEventSeqF;
        this.toFrameSeqF = toFrameSeqF;
        this.supplier = collector.supplier();
        this.accumulator = collector.accumulator();
    }

    public static <T, F> Distributed.Supplier<GroupByFrameP> groupByFrame(
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return groupByFrame(t -> "global", extractTimestampF, toFrameSeqF, collector);
    }

    public static <T, K, F> Distributed.Supplier<GroupByFrameP> groupByFrame(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return () -> new GroupByFrameP<>(extractKeyF, extractTimestampF, toFrameSeqF, collector);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        long eventSeq = extractEventSeqF.applyAsLong(t);
        K key = extractKeyF.apply(t);
        F frame = seqToFrame.computeIfAbsent(toFrameSeqF.applyAsLong(eventSeq), x -> new HashMap<>())
                            .computeIfAbsent(key, x -> supplier.get());
        accumulator.accept(frame, t);
        return true;
    }
}
