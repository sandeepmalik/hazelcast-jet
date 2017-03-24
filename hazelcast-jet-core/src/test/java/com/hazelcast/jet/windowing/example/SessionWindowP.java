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

public class SessionWindowP<T, K, A> extends StreamingProcessorBase {

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Map<K, Session> keyToSession = new HashMap<>();
    private final SortedMap<Long, Map<K, Session>> deadlineToKeyToSession = new TreeMap<>();
    private final Queue<Session> expiredSessionQueue = new ArrayDeque<>();

    public SessionWindowP(
            long maxSeqGap,
            ToLongFunction<? super T> extractEventSeqF,
            Function<? super T, K> extractKeyF,
            DistributedCollector<T, A, ?> collector
    ) {
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.newAccumulatorF = collector.supplier();
        this.accumulateF = collector.accumulator();
        this.maxSeqGap = maxSeqGap;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T event = (T) item;
        K key = extractKeyF.apply(event);
        Session s = keyToSession.computeIfAbsent(key, Session::new);

        // move session in deadline map from old deadline to new deadline
        Map<K, Session> oldDeadlineMap = deadlineToKeyToSession.get(s.expiresAtPunc);
        assert oldDeadlineMap != null;

        if (oldDeadlineMap.size() == 1) {
            deadlineToKeyToSession.remove(s.expiresAtPunc);
        } else {
            oldDeadlineMap.remove(s.key);
        }

        s.accept(event);
        deadlineToKeyToSession.computeIfAbsent(s.expiresAtPunc, x -> new HashMap<>())
                              .put(key, s);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        // move expired sessions from deadline map to expired session queue
        for (Iterator<Map<K, Session>> it = deadlineToKeyToSession.headMap(punc.seq() + 1).values().iterator();
             it.hasNext();
        ) {
            for (Session ses : it.next().values()) {
                expiredSessionQueue.add(ses);
                keyToSession.remove(ses.key);
            }
            it.remove();
        }
        return true;
    }

    @Override
    public void process() {
        for (Session ses; (ses = expiredSessionQueue.poll()) != null;) {
            if (getOutbox().hasReachedLimit()) {
                return;
            }
            emit(new Frame<>(ses.expiresAtPunc, ses.key, ses.acc));
            expiredSessionQueue.remove();
        }
    }

    private final class Session {
        final K key;
        final A acc;
        long expiresAtPunc;

        Session(K key) {
            this.key = key;
            this.acc = newAccumulatorF.get();
        }

        void accept(T event) {
            long eventSeq = extractEventSeqF.applyAsLong(event);
            expiresAtPunc = Math.max(expiresAtPunc, eventSeq + maxSeqGap);
            accumulateF.accept(acc, event);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                    || obj != null
                        && this.getClass() == obj.getClass()
                        && this.key.equals(((Session) obj).key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }
}
