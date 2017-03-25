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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.Traversers.traverseWithRemoval;
import static com.hazelcast.jet.Util.entry;

/**
 * Aggregates events into session windows. Events and windows under
 * different grouping keys are completely independent.
 * <p>
 * Initially a new event causes a new session window to be created. A
 * following event under the same key belongs to this window if its seq is
 * within {@code maxSeqGap} of the window's start seq (in either direction).
 * If the event's seq is less than the window's, it is extended by moving
 * its start seq to the event's seq. Similarly the window's end is adjusted
 * to reach at least up to {@code eventSeq + maxSeqGap}.
 * <p>
 * The event may happen to belong to two existing windows (by bridging the gap
 * between them); in that case they are combined into one.
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
        // `Interval`. Lower and upper window have definitely non-equal intervals,
        // but they may both be equal to the event interval. If they are, the new
        // event belongs to both and therefore causes the two windows to be
        // combined into one.
        A acc;
        Interval resolvedIv;
        Iterator<Entry<Interval, A>> it = ivToAcc.tailMap(eventIv).entrySet().iterator();
        Entry<Interval, A> lowerWindow = it.hasNext() ? it.next() : entry(Interval.NULL, null);
        Entry<Interval, A> upperWindow = it.hasNext() ? it.next() : entry(Interval.NULL, null);
        Interval lowerIv = lowerWindow.getKey();
        Interval upperIv = upperWindow.getKey();
        if (lowerIv.equals(eventIv) && upperIv.equals(eventIv)) {
            ivToAcc.remove(lowerIv);
            ivToAcc.remove(upperIv);
            acc = combineAccF.apply(lowerWindow.getValue(), upperWindow.getValue());
            resolvedIv = new Interval(lowerIv.start, upperIv.end);
            putAbsent(ivToAcc, resolvedIv, acc);
        } else if (lowerIv.equals(eventIv)) {
            acc = lowerWindow.getValue();
            resolvedIv = unite(lowerIv, eventIv, ivToAcc);
        } else if (upperIv.equals(eventIv)) {
            acc = upperWindow.getValue();
            resolvedIv = unite(upperIv, eventIv, ivToAcc);
        } else {
            acc = newAccumulatorF.get();
            resolvedIv = eventIv;
            putAbsent(ivToAcc, resolvedIv, acc);
        }
        accumulateF.accept(acc, event);
        deadlineToKeys.computeIfAbsent(resolvedIv.end, x -> new HashSet<>())
                      .add(key);
        return true;
    }

    private static <K, V> void putAbsent(NavigableMap<K, V> map, K key, V value) {
        assert !map.containsKey(key) : map.keySet() + " already contains " + key;
        map.put(key, value);
    }

    private Interval unite(Interval iv, Interval eventIv, NavigableMap<Interval, A> ivToAcc) {
        if (iv.start <= eventIv.start && iv.end >= eventIv.end) {
            return iv;
        }
        Interval unitedIv = new Interval(Math.min(iv.start, eventIv.start), Math.max(iv.end, eventIv.end));
        A acc = ivToAcc.remove(iv);
        putAbsent(ivToAcc, unitedIv, acc);
        return unitedIv;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        puncSeq = punc.seq();
        return expiredSesFlatmapper.tryProcess(punc);
    }

    /**
     * An interval on the long integer number line. Two intervals are "equal"
     * if they overlap. This deliberately broken definition fails at
     * transitivity, but works well for its single use case: comparing an
     * interval with several other, mutually non-overlapping intervals to
     * find those that overlap it.
     */
    private static class Interval implements Comparable<Interval> {
        final long start;
        final long end;

        static final Interval NULL = new Interval(Long.MIN_VALUE, Long.MIN_VALUE);

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
