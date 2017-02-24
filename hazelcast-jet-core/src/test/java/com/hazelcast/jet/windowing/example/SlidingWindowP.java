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
import com.hazelcast.jet.Distributed.BinaryOperator;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;

/**
 * Javadoc pending.
 */
public class SlidingWindowP<F> extends AbstractProcessor {

    private final SortedMap<Long, F> frames = new TreeMap<>();
    private final BinaryOperator<F> combiner;
    private final int windowSize;
    private final Function<F, ?> finisher;

    public SlidingWindowP(int windowSize, SnapshottingCollector<?, F, ?> sc) {
        this.windowSize = windowSize;
        this.combiner = sc.combiner();
        this.finisher = sc.finisher();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Entry<Long, F> e = (Entry<Long, F>) item;
        final long frameSeq = e.getKey();
        frames.put(frameSeq, e.getValue());
        final long oldestFrameSeq = frames.firstKey();
        if (frameSeq - oldestFrameSeq >= windowSize) {
            return true;
        }
        final SortedMap<Long, F> windowMap = frames.headMap(oldestFrameSeq + windowSize);
        if (windowMap.size() == windowSize) {
            emitWindow(windowMap);
        }
        return true;
    }

    private void emitWindow(SortedMap<Long, F> windowMap) {
        F frame = null;
        long lastSeq = 0;
        for (Entry<Long, F> e : windowMap.entrySet()) {
            frame = (frame == null ? e.getValue() : combiner.apply(frame, e.getValue()));
            lastSeq = e.getKey();
        }
        emit(entry(lastSeq, finisher.apply(frame)));
    }
}
