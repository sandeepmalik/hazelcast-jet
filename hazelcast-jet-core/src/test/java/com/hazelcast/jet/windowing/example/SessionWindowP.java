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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

public class SessionWindowP<T, K, A, R> extends StreamingProcessorBase {

    private final long maxSeqGap;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final Supplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final Function<A, R> finishAccumulationF;
    private final Map<K, Long> keyToDeadline = new HashMap<>();
    private final SortedMap<Long, Map<K, A>> deadlineToKeyToAcc = new TreeMap<>();
    private final FlatMapper<Punctuation, Frame<K, R>> expiredSessFlatmapper;
    private long lastObservedPunc;

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
        this.finishAccumulationF = collector.finisher();
        this.maxSeqGap = maxSeqGap;

        expiredSessFlatmapper = flatMapper(punc ->
                traverseIterableWithRemoval(deadlineToKeyToAcc.headMap(punc.seq() + 1).entrySet())
                        .flatMap(deadlineAndKeyToAcc -> traverseIterable(deadlineAndKeyToAcc.getValue().entrySet())
                                .peek(keyAndAcc -> keyToDeadline.remove(keyAndAcc.getKey()))
                                .map(keyAndAcc -> new Frame<>(
                                        deadlineAndKeyToAcc.getKey(), keyAndAcc.getKey(),
                                        finishAccumulationF.apply(keyAndAcc.getValue())))
                        ));
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T event = (T) item;
        long eventTime = extractEventSeqF.applyAsLong(event);
        // drop late events
        if (eventTime <= lastObservedPunc) {
            return true;
        }
        K key = extractKeyF.apply(event);
        Long oldDeadline = keyToDeadline.get(key);
        Map<K, A> oldDeadlineMap = oldDeadline != null ? deadlineToKeyToAcc.get(oldDeadline) : null;
        A acc = oldDeadline != null ? oldDeadlineMap.get(key) : newAccumulatorF.get();
        accumulateF.accept(acc, event);
        long newDeadline = eventTime + maxSeqGap;
        if (oldDeadline != null && oldDeadline > newDeadline)
            newDeadline = oldDeadline;

        if (oldDeadline != null && newDeadline == oldDeadline) {
            return true;
        }
        // move session in deadline map from old deadline to new deadline
        if (oldDeadlineMap != null) {
            oldDeadlineMap.remove(key);
            if (oldDeadlineMap.isEmpty()) {
                deadlineToKeyToAcc.remove(oldDeadline);
            }
        }
        deadlineToKeyToAcc.computeIfAbsent(newDeadline, x -> new HashMap<>())
                          .put(key, acc);
        keyToDeadline.put(key, newDeadline);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        lastObservedPunc = punc.seq();
        return expiredSessFlatmapper.tryProcess(punc);
    }
}
