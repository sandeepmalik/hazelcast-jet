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

import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static java.util.stream.Collectors.toMap;

/**
 * Contains factory methods for processors dealing with windowing operations.
 */
public final class FrameProcessors {

    private FrameProcessors() {
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
    public static <T, K, F> ProcessorSupplier groupByFrame(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return ProcessorSupplier.of(() -> new GroupByFrameP<>(extractKeyF, extractTimestampF, toFrameSeqF, collector));
    }

    /**
     * Convenience for {@link #groupByFrame(Function, ToLongFunction, LongUnaryOperator, DistributedCollector)}
     * which doesn't group by key.
     */
    public static <T, F> ProcessorSupplier groupByFrame(
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            DistributedCollector<T, F, ?> collector
    ) {
        return groupByFrame(t -> "global", extractTimestampF, toFrameSeqF, collector);
    }

    /**
     * Combines frames received from several upstream instances of
     * {@link GroupByFrameP} into finalized frames. Combines frames into sliding
     * windows. Applies the finisher function to produce its emitted output.
     *
     * @param <K> type of the grouping key
     * @param <F> type of the frame
     * @param <R> type of the result derived from a frame
     */
    public static <K, F, R> ProcessorSupplier slidingWindow(int windowSize, DistributedCollector<K, F, R> collector) {
        return ProcessorSupplier.of(() -> new SlidingWindowP<>(windowSize, collector));
    }

    private static class GroupByFrameP<T, K, F> extends StreamingProcessorBase {
        private final SortedMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
        private final ToLongFunction<? super T> extractEventSeqF;
        private final Function<? super T, K> extractKeyF;
        private final LongUnaryOperator toFrameSeqF;
        private final Supplier<F> supplier;
        private final BiConsumer<F, ? super T> accumulator;
        private final FlatMapper<Punctuation, Object> puncFlatMapper;

        GroupByFrameP(
                Function<? super T, K> extractKeyF,
                ToLongFunction<? super T> extractEventSeqF,
                LongUnaryOperator toFrameSeqF,
                DistributedCollector<? super T, F, ?> collector
        ) {
            this.extractKeyF = extractKeyF;
            this.extractEventSeqF = extractEventSeqF;
            this.toFrameSeqF = toFrameSeqF;
            this.supplier = collector.supplier();
            this.accumulator = collector.accumulator();
            this.puncFlatMapper = flatMapper(punc ->
                    traverseWithRemoval(seqToKeyToFrame.headMap(punc.seq() + 1).entrySet())
                            .flatMap(seqAndFrame -> concat(
                                    traverseIterable(seqAndFrame.getValue().entrySet())
                                            .map(e -> new Frame<>(
                                                    seqAndFrame.getKey(), e.getKey(), e.getValue())),
                                    Traverser.over(new Punctuation(seqAndFrame.getKey())))));
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            T t = (T) item;
            long eventSeq = extractEventSeqF.applyAsLong(t);
            K key = extractKeyF.apply(t);
            F frame = seqToKeyToFrame.computeIfAbsent(toFrameSeqF.applyAsLong(eventSeq), x -> new HashMap<>())
                                     .computeIfAbsent(key, x -> supplier.get());
            accumulator.accept(frame, t);
            return true;
        }

        @Override
        protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
            return puncFlatMapper.tryProcess(punc);
        }

        private static <T> Traverser<T> traverseWithRemoval(Iterable<T> iterable) {
            Iterator<T> iterator = iterable.iterator();
            return () -> {
                if (!iterator.hasNext()) {
                    return null;
                }
                T t = iterator.next();
                assert t != null : "Iterator returned null element";
                iterator.remove();
                return t;
            };
        }
    }

    private static class SlidingWindowP<K, F, R> extends StreamingProcessorBase {
        private final SortedMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
        private final BinaryOperator<F> combiner;
        private final FlatMapper<Punctuation, Frame<K, R>> puncFlatMapper;
        private final int windowSize;

        SlidingWindowP(int windowSize, DistributedCollector<K, F, R> collector) {
            this.windowSize = windowSize;
            this.combiner = collector.combiner();
            Function<F, R> finisher = collector.finisher();
            this.puncFlatMapper = flatMapper(punc -> {
                Map<K, F> mx = seqToKeyToFrame.headMap(punc.seq() + 1)
                                              .values().stream()
                                              .flatMap(m -> m.entrySet().stream())
                                              .collect(toMap(Entry::getKey, Entry::getValue, combiner));
                return traverseIterable(mx.entrySet())
                        .map(e -> new Frame<>(punc.seq(), e.getKey(), finisher.apply(e.getValue())));
            });
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            final Frame<K, F> e = (Frame) item;
            final Long frameSeq = e.getSeq();
            final F frame = e.getValue();
            seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                           .merge(e.getKey(), frame, combiner);
            return true;
        }

        @Override
        protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
            boolean done = puncFlatMapper.tryProcess(punc);
            if (done) {
                seqToKeyToFrame.remove(punc.seq() + 1 - windowSize);
            }
            return done;
        }
    }
}
