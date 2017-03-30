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

import com.hazelcast.internal.util.sort.QuickSorter;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
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
public class SessionWindowArrayP<T, K, A, R> extends StreamingProcessorBase {

    // These two fields are exposed for testing, to check for memory leaks
    final Map<K, Windows> keyToWindows = new HashMap<>();
    SortedMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Function<A, R> finishAccumulationF;
    private final BinaryOperator<A> combineAccF;
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
    public SessionWindowArrayP(
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
        Stream<Session<K, R>> sessions = deadlineToKeys.headMap(punc.seq())
                                                       .values().stream()
                                                       .flatMap(Set::stream)
                                                       .distinct()
                                                       .map(keyToWindows::get)
                                                       .flatMap(wins -> wins.closeWindows(punc.seq()).stream());
        deadlineToKeys = deadlineToKeys.tailMap(punc.seq());
        return traverseStream(sessions);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final T event = (T) item;
        final long eventSeq = extractEventSeqF.applyAsLong(event);
        if (eventSeq < puncSeq) {
            // drop late event
            return true;
        }
        K key = extractKeyF.apply(event);
        keyToWindows.computeIfAbsent(key, Windows::new).addEvent(eventSeq, event);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        puncSeq = punc.seq();
        return expiredSesFlatmapper.tryProcess(punc);
    }

    private class Windows {
        private final K key;
        private int size = 0;
        private long[] starts = new long[2];
        private long[] ends = new long[2];
        private A[] accs = (A[]) new Object[2];

        Windows(K key) {
            this.key = key;
        }

        void addEvent(long eventSeq, T event) {
            long eventEnd = eventSeq + maxSeqGap;
            for (int i = 0; i < size; i++) {
                if (starts[i] <= eventEnd && ends[i] >= eventSeq) {
                    starts[i] = min(starts[i], eventSeq);
                    long oldEnd = ends[i];
                    ends[i] = max(ends[i], eventEnd);
                    if (ends[i] != oldEnd) {
                        deregisterDeadline(oldEnd);
                        registerDeadline(ends[i]);
                    }
                    accumulateF.accept(accs[i], event);
                    return;
                }
            }
            A acc = newAccumulatorF.get();
            accumulateF.accept(acc, event);
            addWindow(eventSeq, eventEnd, acc);
        }

        void addWindow(long start, long end, A acc) {
            registerDeadline(end);
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, 2 * starts.length);
                ends = Arrays.copyOf(ends, 2 * ends.length);
                accs = Arrays.copyOf(accs, 2 * accs.length);
            }
            starts[size] = start;
            ends[size] = end;
            accs[size] = acc;
            size++;
        }

        List<Session<K, R>> closeWindows(long puncSeq) {
            new WinSorter().sort(0, size);
            Windows survivors = new Windows(key);
            List<Session<K, R>> sessions = new ArrayList<>();
            for (int i = 0; i < size;) {
                long start = starts[i];
                long end = ends[i];
                A acc = accs[i];
                for (i++; i < size && starts[i] <= end; i++) {
                    long newEnd = max(end, ends[i]);
                    if (newEnd > end && end >= puncSeq) {
                        deregisterDeadline(end);
                    }
                    end = newEnd;
                    acc = combineAccF.apply(acc, accs[i]);
                }
                if (end < puncSeq) {
                    sessions.add(new Session<>(key, start, end, finishAccumulationF.apply(acc)));
                } else {
                    survivors.addWindow(start, end, acc);
                }
            }
            if (survivors.size > 0) {
                keyToWindows.put(key, survivors);
            } else {
                keyToWindows.remove(key);
            }
            return sessions;
        }

        private boolean registerDeadline(long deadline) {
            return deadlineToKeys.computeIfAbsent(deadline, x -> new HashSet<>()).add(key);
        }

        private void deregisterDeadline(long deadline) {
            Set<K> ks = deadlineToKeys.get(deadline);
            ks.remove(key);
            if (ks.isEmpty()) {
                deadlineToKeys.remove(deadline);
            }
        }

        private class WinSorter extends QuickSorter {
            private long pivot;

            @Override
            protected void loadPivot(long idx) {
                pivot = starts[(int) idx];
            }

            @Override
            protected boolean isLessThanPivot(long idx) {
                return starts[(int) idx] < pivot;
            }

            @Override
            protected boolean isGreaterThanPivot(long idx) {
                return starts[(int) idx] > pivot;
            }

            @Override
            protected void swap(long idx1, long idx2) {
                int i = (int) idx1;
                int j = (int) idx2;

                long tStart = starts[i];
                starts[i] = starts[j];
                starts[j] = tStart;

                long tEnd = ends[i];
                ends[i] = ends[j];
                ends[j] = tEnd;

                A tAcc = accs[i];
                accs[i] = accs[j];
                accs[j] = tAcc;
            }
        }
    }
}
