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

public class SessionWindowP<T, K, A> extends StreamingProcessorBase {

    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final long sessionTimeout;
    private final DistributedCollector<T, A, ?> collector;
    private final long emissionInterval;

    private Map<K, Session> map = new HashMap<>();
    private long now;
    private long nextEmission;

    private Iterator<Entry<K, Session>> emissionIterator;

    public SessionWindowP(ToLongFunction<? super T> extractEventSeqF, Function<? super T, K> extractKeyF, long sessionTimeout,
            DistributedCollector<T, A, ?> collector, long emissionInterval) {
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.sessionTimeout = sessionTimeout;
        this.collector = collector;
        this.emissionInterval = emissionInterval;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item_) throws Exception {
        //noinspection unchecked
        T item = (T) item_;
        K key = extractKeyF.apply(item);
        // note that we might accumulate to windows, that should be already closed, but are not
        // due to full outbox. I don't think this is a concern...
        map.computeIfAbsent(key, k -> new Session())
                .merge(item);

        return true;
    }

    @Override
    protected boolean tryProcessPunc(int ordinal, @Nonnull Punctuation punc) {
        // now is the punc we received. This way we account for input stream lag
        now = punc.seq();
        return true;
    }

    @Override
    public void process() {
        if (emissionIterator == null && nextEmission < now) {
            emissionIterator = map.entrySet().iterator();
        }

        if (emissionIterator != null) {
            // close windows that timed out
            while (emissionIterator.hasNext()) {
                if (getOutbox().hasReachedLimit()) {
                    // If outbox is full, just return without updating nextEmission.
                    // This way, we'll continue after we're called again
                    return;
                }

                Entry<K, Session> entry = emissionIterator.next();
                if (entry.getValue().timeoutAt <= now) {
                    emissionIterator.remove();
                    // frame seq is the end of session
                    emit(new Frame<>(entry.getValue().timeoutAt, entry.getKey(), entry.getValue().accumulator));
                }
            }

            // we are done emitting this iterator
            emissionIterator = null;
            nextEmission = now + emissionInterval;
        }
    }

    private final class Session {
        long timeoutAt;
        A accumulator;

        void merge(T item) {
            long eventSeq = extractEventSeqF.applyAsLong(item);
            timeoutAt = Math.max(timeoutAt, eventSeq + sessionTimeout);
            collector.accumulator().accept(accumulator, item);
        }
    }
}
