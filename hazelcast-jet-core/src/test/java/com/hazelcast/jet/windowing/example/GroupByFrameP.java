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

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.iterate;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.triple;

public class GroupByFrameP<T, K, F, R> extends StreamingProcessorBase {
    private final SnapshottingCollector<? super T, F, R> sc;
    private final ToLongFunction<? super T> extractEventSeqF;
    private final Function<? super T, K> extractKeyF;
    private final LongUnaryOperator toFrameSeqF;
    private final SortedMap<Long, Map<K, F>> seqToFrame = new TreeMap<>();
    private Traverser<Object> frameTraverser;

    private GroupByFrameP(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractEventSeqF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<? super T, F, R> sc
    ) {
        this.sc = sc;
        this.extractEventSeqF = extractEventSeqF;
        this.extractKeyF = extractKeyF;
        this.toFrameSeqF = toFrameSeqF;
    }

    public static <T, F, R> Distributed.Supplier<GroupByFrameP> groupByFrame(
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, F, R> tc
    ) {
        return groupByFrame(t -> "global", extractTimestampF, toFrameSeqF, tc);
    }

    public static <T, K, F, R> Distributed.Supplier<GroupByFrameP> groupByFrame(
            Function<? super T, K> extractKeyF,
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            SnapshottingCollector<T, F, R> tc
    ) {
        return () -> new GroupByFrameP<>(extractKeyF, extractTimestampF, toFrameSeqF, tc);
    }

    @Override
    public void process() {
        tryCompletePendingFrames();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        if (!tryCompletePendingFrames()) {
            return false;
        }
        T t = (T) item;
        long eventSeq = extractEventSeqF.applyAsLong(t);
        K key = extractKeyF.apply(t);
        F frame = seqToFrame.computeIfAbsent(toFrameSeqF.applyAsLong(eventSeq), x -> new HashMap<>())
                            .computeIfAbsent(key, x -> sc.supplier().get());
        sc.accumulator().accept(frame, t);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (!tryCompletePendingFrames()) {
            return false;
        }
        long puncSeq = punc.seq();
        frameTraverser = traverseWithRemoval(seqToFrame.entrySet())
                .takeWhile(seqAndFrame -> seqAndFrame.getKey() <= puncSeq)
                .flatMap(seqAndFrame -> concat(
                        traverseIterable(seqAndFrame.getValue().entrySet())
                                .map(e -> triple(seqAndFrame.getKey(), e.getKey(), e.getValue())),
                        Traverser.over(new Punctuation(seqAndFrame.getKey()))));
        return tryCompletePendingFrames();
    }

    private boolean tryCompletePendingFrames() {
        if (frameTraverser == null) {
            return true;
        }
        boolean done = emitCooperatively(frameTraverser);
        if (done) {
            frameTraverser = null;
        }
        return done;
    }

    private static <T> Traverser<T> traverseWithRemoval(Iterable<T> iterable) {
        Iterator<T> it = iterable.iterator();
        return () -> {
            if (!it.hasNext()) {
                return null;
            }
            T t = it.next();
            it.remove();
            return t;
        };
    }
}
