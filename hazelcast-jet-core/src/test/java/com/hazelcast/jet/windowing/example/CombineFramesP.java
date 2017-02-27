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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

/**
 * Javadoc pending.
 */
public class CombineFramesP<B, R> extends AbstractProcessor {
    private final SnapshottingCollector<?, B, R> tc;
    private final int expectedGroupSize;
    private final Map<Long, List<B>> seqToFrames = new HashMap<>();

    private CombineFramesP(SnapshottingCollector<?, B, R> tc, int expectedGroupSize) {
        this.tc = tc;
        this.expectedGroupSize = expectedGroupSize;
    }

    public static <B, R> Distributed.Supplier<CombineFramesP<B, R>> combineFrames(
            SnapshottingCollector<?, B, R> tc, int expectedGroupSize
    ) {
        return () -> new CombineFramesP<>(tc, expectedGroupSize);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Entry<Long, B> e = (Entry<Long, B>) item;
        final Long frameSeq = e.getKey();
        final B frame = e.getValue();
        final List<B> frameGroup = seqToFrames.computeIfAbsent(frameSeq, fs -> new ArrayList<>());
        frameGroup.add(frame);
        if (frameGroup.size() == expectedGroupSize) {
            B combined = frameGroup.get(0);
            for (int i = 1; i < frameGroup.size(); i++) {
                combined = tc.combiner().apply(combined, frameGroup.get(i));
            }
            emit(entry(frameSeq, tc.finisher().apply(combined)));
            seqToFrames.remove(frameSeq);
        }
        return true;
    }
}
