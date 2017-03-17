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
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.Distributed.Function.identity;
import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

/**
 * Base class with logic common to {@link GroupByFrameP} and
 * {@link CombineFramesP}.
 */
public abstract class FrameProcessors<K, F, R> extends StreamingProcessorBase {
    final SortedMap<Long, Map<K, F>> seqToFrame = new TreeMap<>();
    private final FlatMapper<Punctuation, Object> puncFlatMapper;

    FrameProcessors(Function<F, R> finisher) {
        this.puncFlatMapper = flatMapper((Punctuation punc) ->
                traverseIterableWithRemoval(seqToFrame.headMap(punc.seq() + 1).entrySet())
                        .flatMap(seqAndFrame -> concat(
                                traverseIterable(seqAndFrame.getValue().entrySet())
                                        .map(e -> new KeyedFrame<>(
                                                seqAndFrame.getKey(), e.getKey(), finisher.apply(e.getValue()))),
                                Traverser.over(new Punctuation(seqAndFrame.getKey())))));
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        return puncFlatMapper.tryProcess(punc);
    }


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
    public static <T, K, F> Distributed.Supplier<GroupByFrameP> groupByFrame(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return () -> new GroupByFrameP<>(extractKeyF, extractTimestampF, toFrameSeqF, collector);
    }

    /**
     * Convenience for {@link #groupByFrame(Function, ToLongFunction, LongUnaryOperator, DistributedCollector)}
     * which doesn't group by key.
     */
    public static <T, F> Distributed.Supplier<GroupByFrameP> groupByFrame(
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return groupByFrame(t -> "global", extractTimestampF, toFrameSeqF, collector);
    }

    /**
     * Combines frames received from several upstream instances of
     * {@link GroupByFrameP} into finalized frames. Applies the finisher
     * function to produce its emitted output.
     *
     * @param <K> type of grouping key
     * @param <F> type of the frame
     * @param <R> type of the result derived from a frame
     */
    public static <K, F, R> Distributed.Supplier<CombineFramesP<K, F, R>> combineFrames(
            DistributedCollector<?, F, R> collector
    ) {
        return () -> new CombineFramesP<>(collector);
    }

    static class GroupByFrameP<T, K, F> extends FrameProcessors<K, F, F> {
        private final ToLongFunction<? super T> extractEventSeqF;
        private final Function<? super T, K> extractKeyF;
        private final LongUnaryOperator toFrameSeqF;
        private final Supplier<F> supplier;
        private final BiConsumer<F, ? super T> accumulator;

        GroupByFrameP(
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

    static class CombineFramesP<K, F, R> extends FrameProcessors<K, F, R> {
        private final BinaryOperator<F> combiner;

        CombineFramesP(DistributedCollector<?, F, R> collector) {
            super(collector.finisher());
            this.combiner = collector.combiner();
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            final KeyedFrame<K, F> e = (KeyedFrame) item;
            final Long frameSeq = e.getSeq();
            final F frame = e.getValue();
            seqToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                      .merge(e.getKey(), frame, combiner);
            return true;
        }
    }
}
