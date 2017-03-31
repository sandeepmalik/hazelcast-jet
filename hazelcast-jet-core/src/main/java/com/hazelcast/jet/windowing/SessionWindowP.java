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
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

/**
 * Aggregates events into session windows. Events and windows under
 * different grouping keys are completely independent.
 * <p>
 * The functioning of this processor is easiest to explain in terms of
 * the <em>event interval</em>: the range {@code [eventSeq, eventSeq + maxSeqGap]}.
 * Initially an event causes a new session window to be created, covering
 * exactly the event interval. A following event under the same key belongs
 * to this window iff its interval overlaps it. The window is extended to
 * cover the entire interval of the new event. The event may happen to
 * belong to two existing windows if its interval bridges the gap between
 * them; in that case they are combined into one.
 *
 * @param <T> type of stream event
 * @param <K> type of event's grouping key
 * @param <A> type of the container of accumulated value
 * @param <R> type of the result value for a session window
 */
public class SessionWindowP<T, K, A, R> extends StreamingProcessorBase {

    // exposed for testing, to check for memory leaks
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
        Stream<Session<K, R>> sessions = deadlineToKeys
                .headMap(punc.seq())
                .values().stream()
                .flatMap(Set::stream)
                .distinct()
                .map(key -> keyToWindows.get(key).closeWindows(key, punc.seq()))
                .flatMap(List::stream);
        deadlineToKeys = deadlineToKeys.tailMap(punc.seq());
        return traverseStream(sessions);
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        puncSeq = punc.seq();
        return expiredSesFlatmapper.tryProcess(punc);
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
        keyToWindows.computeIfAbsent(key, k -> new Windows())
                    .addEvent(key, eventSeq, event);
        return true;
    }

    private class Windows {
        private int size;
        private long[] starts = new long[2];
        private long[] ends = new long[2];
        private A[] accs = (A[]) new Object[2];

        void addEvent(K key, long eventSeq, T event) {
            accumulateF.accept(resolveAcc(key, eventSeq), event);
        }

        List<Session<K, R>> closeWindows(K key, long puncSeq) {
            List<Session<K, R>> sessions = new ArrayList<>();
            int i = 0;
            for (; i < size && ends[i] < puncSeq; i++) {
                sessions.add(new Session<>(key, starts[i], ends[i], finishAccumulationF.apply(accs[i])));
            }
            if (i != size) {
                deleteHead(i);
            } else {
                keyToWindows.remove(key);
            }
            return sessions;
        }

        private void deleteHead(int deletedCount) {
            copy(deletedCount, 0, size - deletedCount);
            size -= deletedCount;
        }

        private A resolveAcc(K key, long eventSeq) {
            long eventEnd = eventSeq + maxSeqGap;
            int i = 0;
            for (; i < size && starts[i] <= eventEnd; i++) {
                // this window is not after the event interval

                if (ends[i] < eventSeq) {
                    // this window is before the event interval
                    continue;
                }
                if (starts[i] <= eventSeq && ends[i] >= eventEnd) {
                    // this window fully covers the event interval
                    return accs[i];
                }
                // this window overlaps the event interval

                if (i + 1 == size || starts[i + 1] > eventEnd) {
                    // next window doesn't overlap the event interval
                    starts[i] = min(starts[i], eventSeq);
                    if (ends[i] < eventEnd) {
                        removeFromDeadlines(key, ends[i]);
                        ends[i] = eventEnd;
                        addToDeadlines(key, ends[i]);
                    }
                    return accs[i];
                }
                // event belongs to both this and next window
                removeFromDeadlines(key, ends[i]);
                ends[i] = ends[i + 1];
                accs[i] = combineAccF.apply(accs[i], accs[i + 1]);
                deleteWindow(i + 1);
                return accs[i];
            }
            addToDeadlines(key, eventEnd);
            return insertWindow(i, eventSeq, eventEnd);
        }

        private void deleteWindow(int idx) {
            size--;
            copy(idx + 1, idx, size - idx);
        }

        private A insertWindow(int idx, long eventSeq, long eventEnd) {
            expandIfNeeded();
            copy(idx, idx + 1, size - idx);
            size++;
            starts[idx] = eventSeq;
            ends[idx] = eventEnd;
            accs[idx] = newAccumulatorF.get();
            return accs[idx];
        }

        private void addToDeadlines(K key, long deadline) {
            deadlineToKeys.computeIfAbsent(deadline, x -> new HashSet<>()).add(key);
        }

        private void removeFromDeadlines(K key, long deadline) {
            Set<K> ks = deadlineToKeys.get(deadline);
            ks.remove(key);
            if (ks.isEmpty()) {
                deadlineToKeys.remove(deadline);
            }
        }

        private void copy(int from, int to, int length) {
            arraycopy(starts, from, starts, to, length);
            arraycopy(ends, from, ends, to, length);
            arraycopy(accs, from, accs, to, length);
        }

        private void expandIfNeeded() {
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, 2 * starts.length);
                ends = Arrays.copyOf(ends, 2 * ends.length);
                accs = Arrays.copyOf(accs, 2 * accs.length);
            }
        }
    }
}
