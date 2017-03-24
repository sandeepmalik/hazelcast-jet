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

import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.Traversers.traverseIterable;

public class SessionWindowP<T, K, A, R> extends StreamingProcessorBase {

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Function<A, R> finishAccumulationF;
    private final Map<K, Session> keyToSession = new HashMap<>();
    private final SortedMap<Long, Map<K, Session<A>>> deadlineToKeyToSession = new TreeMap<>();
    private final Queue<Map<K, Session<A>>> expiredSessionQueue = new ArrayDeque<>();
    private final FlatMapper<Object, Frame<K, R>> expiredSesFlatmapper;

    public SessionWindowP(
            long maxSeqGap,
            ToLongFunction<? super T> extractEventSeqF,
            Function<? super T, K> extractKeyF,
            DistributedCollector<T, A, R> collector
    ) {
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.newAccumulatorF = collector.supplier();
        this.accumulateF = collector.accumulator();
        this.finishAccumulationF = collector.finisher();
        this.maxSeqGap = maxSeqGap;
        Traverser<Map<K, Session<A>>> trav = expiredSessionQueue::poll;
        expiredSesFlatmapper = flatMapper(x ->
                trav.flatMap(sesMap -> traverseIterable(sesMap.entrySet()))
                    .map(entry -> new Frame<>(entry.getValue().expiresAtPunc, entry.getKey(), finishAccumulationF.apply(entry.getValue().acc)))
        );
    }

    @Override
    public void process() {
        expiredSesFlatmapper.tryProcess(0);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        if (!expiredSesFlatmapper.tryProcess(0)) {
            return false;
        }
        T event = (T) item;
        K key = extractKeyF.apply(event);
        Session<A> s = keyToSession.computeIfAbsent(key, k -> new Session(newAccumulatorF.get()));
        long oldDeadline = s.expiresAtPunc;
        accumulateF.accept(s.acc, event);
        long eventSeq = extractEventSeqF.applyAsLong(event);
        s.expiresAtPunc = Math.max(s.expiresAtPunc, eventSeq + maxSeqGap);
        long newDeadline = s.expiresAtPunc;
        assert newDeadline != Long.MIN_VALUE : "Broken event time: " + extractEventSeqF.applyAsLong(event);
        if (newDeadline == oldDeadline) {
            return true;
        }
        // move session in deadline map from old deadline to new deadline
        Map<K, Session<A>> oldDeadlineMap = deadlineToKeyToSession.get(oldDeadline);
        if (oldDeadlineMap != null) {
            oldDeadlineMap.remove(key);
            if (oldDeadlineMap.isEmpty()) {
                deadlineToKeyToSession.remove(oldDeadline);
            }
        }
        deadlineToKeyToSession.computeIfAbsent(newDeadline, x -> new HashMap<>())
                              .put(key, s);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        // move expired session maps from deadline map to expired session queue
        for (Iterator<Map<K, Session<A>>> it = deadlineToKeyToSession.headMap(punc.seq() + 1).values().iterator();
             it.hasNext();
        ) {
            Map<K, Session<A>> sesMap = it.next();
            expiredSessionQueue.add(sesMap);
            it.remove();
            sesMap.keySet().forEach(keyToSession::remove);
        }
        return true;
    }

    private static final class Session<A> {
        final A acc;
        long expiresAtPunc = Long.MIN_VALUE;

        private Session(A acc) {
            this.acc = acc;
        }
    }
}
