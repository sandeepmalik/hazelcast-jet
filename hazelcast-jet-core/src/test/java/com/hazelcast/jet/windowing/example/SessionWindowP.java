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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

public class SessionWindowP<T, K, A, R> extends StreamingProcessorBase {

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Function<A, R> finishAccumulationF;
    private final Map<K, Long> keyToDeadline = new HashMap<>();
    private final SortedMap<Long, Map<K, A>> deadlineToKeyToSession = new TreeMap<>();
    private Traverser<Frame<K, R>> expiredSesTraverser;

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
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T event = (T) item;
        K key = extractKeyF.apply(event);
        Long oldDeadline = keyToDeadline.get(key);
        Map<K, A> oldDeadlineMap = oldDeadline != null ? deadlineToKeyToSession.get(oldDeadline) : null;
        A acc = oldDeadline != null ? oldDeadlineMap.get(key) : newAccumulatorF.get();
        accumulateF.accept(acc, event);
        long newDeadline = extractEventSeqF.applyAsLong(event) + maxSeqGap;
        if (oldDeadline != null && oldDeadline > newDeadline)
            newDeadline = oldDeadline;

        if (oldDeadline != null && newDeadline == oldDeadline) {
            return true;
        }
        // move session in deadline map from old deadline to new deadline
        if (oldDeadlineMap != null) {
            oldDeadlineMap.remove(key);
            if (oldDeadlineMap.isEmpty()) {
                deadlineToKeyToSession.remove(oldDeadline);
            }
        }
        deadlineToKeyToSession.computeIfAbsent(newDeadline, x -> new HashMap<>())
                              .put(key, acc);
        keyToDeadline.put(key, newDeadline);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (expiredSesTraverser != null) {
            if (emitCooperatively(expiredSesTraverser)) {
                expiredSesTraverser = null;
            } else {
                return false;
            }
        }

        expiredSesTraverser =
                Traversers.traverseIterable(deadlineToKeyToSession.headMap(punc.seq() + 1).entrySet())
                        .flatMap(entry -> Traversers.traverseIterable(entry.getValue().entrySet())
                                .map(entry2 -> new Frame<>(entry.getKey(), entry2.getKey(), finishAccumulationF.apply(entry2.getValue()))));

        // we don't consume the punc, until we are done emitting
        return false;
    }
}
