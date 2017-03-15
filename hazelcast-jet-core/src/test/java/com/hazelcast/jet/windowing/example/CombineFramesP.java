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
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

/**
 * Javadoc pending.
 */
public class CombineFramesP<K, F, R> extends StreamingProcessorBase {
    private final SnapshottingCollector<?, F, R> tc;
    private final SortedMap<Long, Map<K, F>> seqToFrame = new TreeMap<>();
    private Traverser<Object> frameTraverser;

    private CombineFramesP(SnapshottingCollector<?, F, R> tc) {
        this.tc = tc;
    }

    public static <K, B, R> Distributed.Supplier<CombineFramesP<K, B, R>> combineFrames(
            SnapshottingCollector<?, B, R> tc
    ) {
        return () -> new CombineFramesP<>(tc);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final KeyedFrame<K, F> e = (KeyedFrame) item;
        final Long frameSeq = e.getSeq();
        final F frame = e.getValue();
        seqToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                       .compute(e.getKey(),
                               (s, f) -> tc.combiner().apply(
                                       f != null ? f : tc.supplier().get(),
                                       frame)
                       );
        return true;
    }

    @Override
    protected boolean tryProcessPunc(int ordinal, @Nonnull Punctuation punc) {
        if (!tryCompletePendingFrames()) {
            return false;
        }

        Map<K, F> keys = seqToFrame.remove(punc.seq());
        if (keys == null) {
            return true;
        }

        frameTraverser = traverseIterableWithRemoval(seqToFrame.headMap(punc.seq() + 1).entrySet())
                .flatMap(seqAndFrame -> concat(
                        traverseIterable(seqAndFrame.getValue().entrySet())
                                .map(e -> new KeyedFrame<>(seqAndFrame.getKey(), e.getKey(), tc.finisher().apply(e.getValue()))),
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
}
