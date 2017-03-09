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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Optional;
import com.hazelcast.jet.Punctuation;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Javadoc pending.
 */
public class CombineFramesP<K, F, R> extends AbstractProcessor {
    private final SnapshottingCollector<?, F, R> tc;
    private final Map<Long, Map<K, F>> seqToKeyToFrame = new HashMap<>();

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
        seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                       .compute(e.getKey(),
                               (s, f) -> tc.combiner().apply(
                                       Optional.ofNullable(f).orElseGet(tc.supplier()),
                                       frame)
                       );
        return true;
    }

    @Override
    public boolean tryProcessPunctuation(int ordinal, Punctuation punc) {
        SeqPunctuation frame = (SeqPunctuation) punc;
        Map<K, F> keys = seqToKeyToFrame.remove(frame.seq());
        if (keys == null) {
            return true;
        }
        for (Entry<K, F> entry : keys.entrySet()) {
            emit(new KeyedFrame<>(frame.seq(), entry.getKey(), tc.finisher().apply(entry.getValue())));
        }
        return true;
    }
}
