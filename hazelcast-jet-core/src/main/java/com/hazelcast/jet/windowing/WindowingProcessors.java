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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseWithRemoval;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.function.Function.identity;

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
    public static <T, K, F> Distributed.Supplier<GroupByFrameP<T, K, F>> groupByFrame(
            Distributed.Function<? super T, K> extractKeyF,
            Distributed.ToLongFunction<? super T> extractTimestampF,
            long frameLength,
            long frameOffset,
            DistributedCollector<T, F, ?> collector
    ) {
        return () -> new GroupByFrameP<>(extractKeyF, extractTimestampF, frameLength, frameOffset, collector);
    }

    /**
     * Convenience for {@link #groupByFrame(
     * Distributed.Function, Distributed.ToLongFunction, long, long, DistributedCollector)
     * groupByFrame(extractKeyF, extractTimeStampF, frameLength, frameOffset, collector)}
     * which doesn't group by key.
     */
    public static <T, F> Distributed.Supplier<GroupByFrameP<T, String, F>> groupByFrame(
            Distributed.ToLongFunction<? super T> extractTimestampF,
            long frameLength,
            long frameOffset,
            DistributedCollector<T, F, ?> collector
    ) {
        return groupByFrame(t -> "global", extractTimestampF, frameLength, frameOffset, collector);
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
    public static <K, F, R> Distributed.Supplier<SlidingWindowP<K, F, R>> slidingWindow(
            long frameLength, long framesPerWindow, DistributedCollector<K, F, R> collector) {
        return () -> new SlidingWindowP<>(frameLength, framesPerWindow, collector);
    }

    /**
     * Group-by-frame processor, the first stage for sliding and tumbling
     * windows.
     *
     * @param <T> type of item
     * @param <K> type of grouping key
     * @param <F> type of the accumulated result in the frame
     */
    public static class GroupByFrameP<T, K, F> extends StreamingProcessorBase {
        final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
        private final ToLongFunction<? super T> extractEventSeqF;
        private final Function<? super T, K> extractKeyF;
        private final LongUnaryOperator toFrameSeqF;
        private final Supplier<F> supplier;
        private final BiConsumer<F, ? super T> accumulator;
        private final FlatMapper<Punctuation, Object> puncFlatMapper;

        GroupByFrameP(
                Function<? super T, K> extractKeyF,
                ToLongFunction<? super T> extractEventSeqF,
                long frameLength,
                long frameOffset,
                DistributedCollector<? super T, F, ?> collector
        ) {
            checkPositive(frameLength, "frameLength must be positive");
            checkTrue(frameOffset >= 0 && frameOffset < frameLength,
                    "frameOffset must be 0..frameLength-1");
            this.extractKeyF = extractKeyF;
            this.extractEventSeqF = extractEventSeqF;
            this.toFrameSeqF = time -> time - Math.floorMod(time - frameOffset, frameLength) + frameLength;
            this.supplier = collector.supplier();
            this.accumulator = collector.accumulator();
            this.puncFlatMapper = flatMapper(this::emitFrames);
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            T t = (T) item;
            long eventSeq = extractEventSeqF.applyAsLong(t);
            long frameSeq = toFrameSeqF.applyAsLong(eventSeq);
            K key = extractKeyF.apply(t);
            F frame = seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                                     .computeIfAbsent(key, x -> supplier.get());
            accumulator.accept(frame, t);
            return true;
        }

        @Override
        protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
            return puncFlatMapper.tryProcess(punc);
        }

        private Traverser<Object> emitFrames(Punctuation punc) {
            return traverseWithRemoval(seqToKeyToFrame.headMap(punc.seq(), true).entrySet())
                    .<Object>flatMap(seqAndFrame ->
                            traverseIterable(seqAndFrame.getValue().entrySet())
                                    .map(e -> new Frame<>(seqAndFrame.getKey(), e.getKey(), e.getValue())))
                    .append(punc);
        }
    }

    /**
     * Sliding window processor, second stage that receives input from
     * {@link GroupByFrameP}.
     *
     * @param <K> type of key
     * @param <F> type of the accumulated value in the frame
     * @param <R> type of the final result
     */
    public static class SlidingWindowP<K, F, R> extends StreamingProcessorBase {
        private final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
        private final FlatMapper<Punctuation, Object> flatMapper;
        private final BinaryOperator<F> combiner;
        private final Function<F, R> finisher;
        private final Supplier<F> supplier;
        private final long frameLength;
        private final long windowLength;

        private long nextFrameSeqToEmit = Long.MIN_VALUE;

        SlidingWindowP(long frameLength, long framesPerWindow, @Nonnull DistributedCollector<K, F, R> collector) {
            checkPositive(frameLength, "frameLength must be positive");
            checkPositive(framesPerWindow, "framesPerWindow must be positive");
            this.frameLength = frameLength;
            this.windowLength = frameLength * framesPerWindow;
            this.supplier = collector.supplier();
            this.combiner = collector.combiner();
            this.finisher = collector.finisher();
            this.flatMapper = flatMapper(this::slideTheWindow);
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
            if (nextFrameSeqToEmit == Long.MIN_VALUE) {
                if (seqToKeyToFrame.isEmpty()) {
                    return true;
                }
                nextFrameSeqToEmit = seqToKeyToFrame.firstKey();
            }
            return flatMapper.tryProcess(punc);
        }

        private Traverser<Object> slideTheWindow(Punctuation punc) {
            return Traversers.<Object>traverseStream(
                range(nextFrameSeqToEmit, updateAndGetNextFrameSeq(punc.seq()), frameLength)
                    .mapToObj(frameSeq -> {
                        Map<K, F> window = computeWindow(frameSeq);
                        seqToKeyToFrame.remove(frameSeq - windowLength);
                        return window.entrySet().stream()
                                     .map(e -> new Frame<>(frameSeq, e.getKey(), finisher.apply(e.getValue())));
                    }).flatMap(identity()))
                .append(punc);
        }

        private long updateAndGetNextFrameSeq(long puncSeq) {
            long deltaToPunc = puncSeq - nextFrameSeqToEmit;
            if (deltaToPunc < 0) {
                return nextFrameSeqToEmit;
            }
            long frameSeqDelta = deltaToPunc - deltaToPunc % frameLength;
            nextFrameSeqToEmit += frameSeqDelta + frameLength;
            return nextFrameSeqToEmit;
        }

        private Map<K, F> computeWindow(long frameSeq) {
            Map<K, F> window = new HashMap<>();
            Map<Long, Map<K, F>> frames = seqToKeyToFrame.subMap(frameSeq - windowLength, false, frameSeq, true);
            for (Map<K, F> keyToFrame : frames.values()) {
                keyToFrame.forEach((key, currAcc) ->
                        window.compute(key, (x, acc) -> combiner.apply(acc != null ? acc : supplier.get(), currAcc)));
            }
            return window;
        }

        private static LongStream range(long start, long end, long step) {
            return start >= end
                    ? LongStream.empty()
                    : LongStream.iterate(start, n -> n + step).limit(1 + (end - start - 1) / step);
        }
    }
}
