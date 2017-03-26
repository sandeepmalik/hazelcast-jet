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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
import static java.lang.Math.max;
import static java.lang.Math.min;

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
    private final NavigableMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();
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
        final Interval deadlineIv = new Interval(punc.seq(), punc.seq());
        return traverseWithRemoval(deadlineToKeys.headMap(punc.seq(), true).values())
                .flatMap(Traversers::traverseIterable)
                .flatMap(k -> traverseWithRemoval(keyToIvToAcc.get(k).headMap(deadlineIv, true).entrySet())
                        .map(ivAndAcc -> new Session<>(
                                k, finishAccumulationF.apply(ivAndAcc.getValue()),
                                ivAndAcc.getKey().start, ivAndAcc.getKey().end))
                        .onNull(() -> {
                            if (keyToIvToAcc.get(k).isEmpty()) {
                                keyToIvToAcc.remove(k);
                            }
                        })
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
        K key = extractKeyF.apply(event);
        NavigableMap<Interval, A> ivToAcc = keyToIvToAcc.get(key);
        Interval eventIv = new Interval(eventSeq, eventSeq + maxSeqGap);
        if (ivToAcc == null) {
            A acc = newAccumulatorF.get();
            accumulateF.accept(acc, event);
            ivToAcc = new TreeMap<>();
            putAbsent(ivToAcc, key, entry(eventIv, acc));
            keyToIvToAcc.put(key, ivToAcc);
            return true;
        }
        Entry<Interval, A> resolvedWin = resolveWindow(ivToAcc, key, eventIv);
        accumulateF.accept(resolvedWin.getValue(), event);
        return true;
    }

    // This logic relies on the non-transitive equality relation defined for
    // `Interval`. Lower and upper windows have definitely non-equal intervals,
    // but they may both be equal to the event interval. If they are, the new
    // event belongs to both and causes the two windows to be combined into one.
    // Further note that at most two existing intervals can overlap the event
    // interval because they are at least as large as it.
    private Entry<Interval, A> resolveWindow(NavigableMap<Interval, A> ivToAcc, K key, Interval eventIv) {
        Iterator<Entry<Interval, A>> it = ivToAcc.tailMap(eventIv).entrySet().iterator();
        Entry<Interval, A> lowerWindow = overlappingOrNull(it, eventIv);
        if (lowerWindow == null) {
            return putAbsent(ivToAcc, key, entry(eventIv, newAccumulatorF.get()));
        }
        Interval lowerIv = lowerWindow.getKey();
        Entry<Interval, A> upperWindow = overlappingOrNull(it, eventIv);
        if (upperWindow != null) {
            Interval upperIv = upperWindow.getKey();
            delete(ivToAcc, key, lowerIv);
            delete(ivToAcc, key, upperIv);
            return putAbsent(ivToAcc, key, entry(
                    new Interval(lowerIv.start, upperIv.end),
                    combineAccF.apply(lowerWindow.getValue(), upperWindow.getValue()))
            );
        }
        if (encompasses(lowerIv, eventIv)) {
            return lowerWindow;
        }
        delete(ivToAcc, key, lowerIv);
        return putAbsent(ivToAcc, key, entry(union(lowerIv, eventIv), lowerWindow.getValue()));
    }

    private Entry<Interval, A> overlappingOrNull(Iterator<Entry<Interval, A>> it, Interval eventIv) {
        if (!it.hasNext()) {
            return null;
        }
        Entry<Interval, A> win = it.next();
        return eventIv.equals(win.getKey()) ? win : null;
    }

    private static boolean encompasses(Interval outer, Interval inner) {
        return outer.start <= inner.start && outer.end >= inner.end;
    }

    private static Interval union(Interval iv1, Interval iv2) {
        return new Interval(min(iv1.start, iv2.start), max(iv1.end, iv2.end));
    }

    private void delete(NavigableMap<Interval, A> ivToAcc, K key, Interval lowerIv) {
        ivToAcc.remove(lowerIv);
        Set<K> keys = deadlineToKeys.get(lowerIv.end);
        keys.remove(key);
        if (keys.isEmpty()) {
            deadlineToKeys.remove(lowerIv.end);
        }
    }

    private Entry<Interval, A> putAbsent(NavigableMap<Interval, A> ivToAcc, K key, Entry<Interval, A> win) {
        A prev = ivToAcc.put(win.getKey(), win.getValue());
        assert prev == null
                : "Broken interval map implementation: " + win.getKey() + " already present in " + ivToAcc.keySet();
        deadlineToKeys.computeIfAbsent(win.getKey().end, x -> new HashSet<>())
                      .add(key);
        return win;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        puncSeq = punc.seq();
        return expiredSesFlatmapper.tryProcess(punc);
    }

    /**
     * An interval on the long integer number line. Two intervals are "equal"
     * iff they overlap. This deliberately broken definition fails at
     * transitivity, but works well for its single use case: maintaining a
     * {@code TreeMap} of strictly non-overlapping (non-equal) intervals and
     * testing whether a given interval overlaps some of those.
     */
    @SuppressWarnings("equalshashcode")
    @SuppressFBWarnings(value = "HE_EQUALS_USE_HASHCODE", justification = "Not to be used in a hashtable")
    private static class Interval implements Comparable<Interval> {
        final long start;
        final long end;

        Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Interval && compareTo((Interval) obj) == 0;
        }

        @Override
        public int compareTo(@Nonnull Interval that) {
            return this.end < that.start ? -1
                 : that.end < this.start ? 1
                 : 0;
        }

        @Override
        public String toString() {
            return "[" + start + ".." + end + ']';
        }
    }
}
