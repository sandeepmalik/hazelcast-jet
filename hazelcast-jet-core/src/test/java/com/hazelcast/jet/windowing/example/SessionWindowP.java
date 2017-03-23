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
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

public class SessionWindowP<T, K, A> extends StreamingProcessorBase {

    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final long maxSeqGap;
    private final long emissionInterval;

    private Map<K, Session> keyToSession = new HashMap<>();
    private long lastPunc;
    private long nextEmission;

    private Iterator<Entry<K, Session>> sessionIterator;

    public SessionWindowP(ToLongFunction<? super T> extractEventSeqF, Function<? super T, K> extractKeyF,
                          long maxSeqGap, DistributedCollector<T, A, ?> collector, long emissionInterval
    ) {
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.accumulateF = collector.accumulator();
        this.maxSeqGap = maxSeqGap;
        this.emissionInterval = emissionInterval;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        K key = extractKeyF.apply(t);
        // note that we might accumulate to windows, that should be already closed, but are not
        // due to full outbox. I don't think this is a concern...
        keyToSession.computeIfAbsent(key, k -> new Session())
                    .merge(t);

        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        // now is the punc we received. This way we account for input stream lag
        lastPunc = punc.seq();
        return true;
    }

    @Override
    public void process() {
        if (sessionIterator == null) {
            if (lastPunc <= nextEmission) {
                return;
            }
            sessionIterator = keyToSession.entrySet().iterator();
        }

        // close windows that timed out
        while (sessionIterator.hasNext()) {
            if (getOutbox().hasReachedLimit()) {
                return;
            }

            Entry<K, Session> entry = sessionIterator.next();
            if (entry.getValue().closeAtPunc <= lastPunc) {
                sessionIterator.remove();
                // frame seq is the end of session
                emit(new Frame<>(entry.getValue().closeAtPunc, entry.getKey(), entry.getValue().acc));
            }
        }

        // we are done emitting this iterator
        sessionIterator = null;
        nextEmission = lastPunc + emissionInterval;
    }

    private final class Session {
        long closeAtPunc;
        A acc;

        void merge(T item) {
            long eventSeq = extractEventSeqF.applyAsLong(item);
            closeAtPunc = Math.max(closeAtPunc, eventSeq + maxSeqGap);
            accumulateF.accept(acc, item);
        }
    }
}
