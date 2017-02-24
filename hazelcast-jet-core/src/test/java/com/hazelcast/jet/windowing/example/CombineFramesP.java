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
public class CombineFramesP<F, R> extends AbstractProcessor {
    private final TwoTieredCollector<?, ?, F, R> tc;
    private final int expectedGroupSize;
    private final Map<Long, List<F>> seqToFrames = new HashMap<>();

    public CombineFramesP(TwoTieredCollector<?, ?, F, R> tc, int expectedGroupSize) {
        this.tc = tc;
        this.expectedGroupSize = expectedGroupSize;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Entry<Long, F> e = (Entry<Long, F>) item;
        final Long frameSeq = e.getKey();
        final F frame = e.getValue();
        final List<F> frameGroup = seqToFrames.computeIfAbsent(frameSeq, fs -> new ArrayList<>());
        frameGroup.add(frame);
        if (frameGroup.size() == expectedGroupSize) {
            F combined = frameGroup.get(0);
            for (int i = 1; i < frameGroup.size(); i++) {
                combined = tc.combiner().apply(combined, frameGroup.get(i));
            }
            emit(entry(frameSeq, tc.finisher().apply(combined)));
            seqToFrames.remove(frameSeq);
        }
        return true;
    }
}
