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

import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.Traversers.traverseWithRemoval;

/**
 * Aggregates events into session windows. Events under different grouping
 * keys are completely independent, so there is a separate window for each
 * key. A newly observed event will be placed into the existing session
 * window if:
 * <ol><li>
 * it is not behind the punctuation (that is, it is not a late event)
 * </li><li>
 * its {@code eventSeq} is less than {@code maxSeqGap} ahead of the top
 * {@code eventSeq} in the currently maintained window.
 * </li></ol>
 * If the event satisfies 1. but fails 2., the current window will be closed
 * and emitted as a final result, and a new window will be opened with the
 * current event.
 *
 * @param <T> type of stream event
 * @param <K> type of event's grouping key
 * @param <A> type of the container of accumulated value
 * @param <R> type of the result value for a session window
 */
public class SessionWindowP<T, K, A, R> extends StreamingProcessorBase {

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Function<A, R> finishAccumulationF;
    private final BinaryOperator<A> combineAccF;
    private final Map<K, NavigableMap<Interval, A>> keyToIvToAcc = new HashMap<>();
    private final SortedMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();
    private final FlatMapper<Punctuation, Session<K, R>> expiredSesFlatmapper;
    private long puncSeq;

    /**
     * Constructs a session window processor.
     *
     * @param maxSeqGap        maximum gap between consecutive events in the same session window
     * @param extractEventSeqF function to extract the event seq from the event item
     * @param extractKeyF      function to extract the grouping key from the event iem
     * @param collector        contains aggregation logic
     */
    public SessionWindowP(
            long maxSeqGap,
            ToLongFunction<? super T> extractEventSeqF,
            Function<? super T, K> extractKeyF,
            DistributedCollector<? super T, A, R> collector
    ) {
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.newAccumulatorF = collector.supplier();
        this.accumulateF = collector.accumulator();
        this.combineAccF = collector.combiner();
        this.finishAccumulationF = collector.finisher();
        this.maxSeqGap = maxSeqGap;
        this.expiredSesFlatmapper = flatMapper(this::closedWindowTraverser);
    }

    private Traverser<Session<K, R>> closedWindowTraverser(Punctuation punc) {
        final Interval deadlineIv = new Interval(punc.seq() + 1, punc.seq() + 1);
        return traverseWithRemoval(deadlineToKeys.headMap(punc.seq() + 1).values())
                .flatMap(Traversers::traverseIterable)
                .flatMap(k -> traverseWithRemoval(keyToIvToAcc.get(k).headMap(deadlineIv).entrySet())
                        .map(ivAndAcc -> new Session<>(
                                k, finishAccumulationF.apply(ivAndAcc.getValue()),
                                ivAndAcc.getKey().start, ivAndAcc.getKey().end))
                );
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final T event = (T) item;
        final long eventSeq = extractEventSeqF.applyAsLong(event);
        // drop late events
        if (eventSeq <= puncSeq) {
            return true;
        }
        final K key = extractKeyF.apply(event);
        NavigableMap<Interval, A> ivToAcc = keyToIvToAcc.get(key);
        Interval eventIv = new Interval(eventSeq, eventSeq + maxSeqGap);
        if (ivToAcc == null) {
            A acc = newAccumulatorF.get();
            accumulateF.accept(acc, event);
            ivToAcc = new TreeMap<>();
            ivToAcc.put(eventIv, acc);
            keyToIvToAcc.put(key, ivToAcc);
            deadlineToKeys.computeIfAbsent(eventIv.end, x -> new HashSet<>())
                          .add(key);
            return true;
        }
        // This logic relies on the non-transitive equality relation defined for
        // `Interval`. Upper and lower window have definitely non-equal intervals,
        // but they may both be equal to the event interval. If they are, the new
        // event belongs to both and therefore causes the two windows to be
        // combined into one.
        // floorEntry := greatest item less than on equal to eventIv
        // ceilingEtry := least item greater than or equal to eventIv
        A acc;
        Interval resolvedIv;
        Entry<Interval, A> upperWindow = ivToAcc.floorEntry(eventIv);
        Entry<Interval, A> lowerWindow = ivToAcc.ceilingEntry(eventIv);
        Interval upperIv = upperWindow.getKey();
        Interval lowerIv = lowerWindow.getKey();
        if (upperIv.equals(eventIv) && lowerIv.equals(eventIv)) {
            ivToAcc.remove(upperIv);
            ivToAcc.remove(lowerIv);
            acc = combineAccF.apply(upperWindow.getValue(), lowerWindow.getValue());
            resolvedIv = new Interval(lowerIv.start, upperIv.end);
            ivToAcc.put(resolvedIv, acc);
        } else if (upperIv.equals(eventIv)) {
            resolvedIv = upperIv;
            acc = upperWindow.getValue();
        } else if (lowerIv.equals(eventIv)) {
            resolvedIv = lowerIv;
            acc = lowerWindow.getValue();
        } else {
            assert !ivToAcc.containsKey(eventIv) : "Broken interval map implementation: "
                    + ivToAcc.keySet() + " contains " + eventIv;
            resolvedIv = eventIv;
            acc = newAccumulatorF.get();
            ivToAcc.put(eventIv, acc);
        }
        accumulateF.accept(acc, event);
        deadlineToKeys.computeIfAbsent(resolvedIv.end, x -> new HashSet<>())
                      .add(key);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        puncSeq = punc.seq();
        return expiredSesFlatmapper.tryProcess(punc);
    }

    /**
     * An interval on the long integer number line. Has a deliberately broken
     * {@link #equals(Object)} and {@link #compareTo(Interval)} definitions
     * that fail at transitivity, but work well for its single use case:
     * comparing an interval with several other, mutually non-overlapping
     * intervals to find those that overlap it.
     */
    private static class Interval implements Comparable<Interval> {
        final long start;
        final long end;

        Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Interval)) {
                return false;
            }
            final Interval that = (Interval) obj;
            return that.end >= this.start && this.end >= that.start;
        }

        @Override
        public int compareTo(@Nonnull Interval that) {
            return that.end < this.start ? -1
                 : this.end < that.start ? 1
                 : 0;
        }

        @Override
        public String toString() {
            return "[" + start + ".." + end + ')';
        }
    }
}
