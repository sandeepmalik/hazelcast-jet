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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WatermarkAwareProcessor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

/**
 * Javadoc pending.
 */
public class CombineFramesP<K, B, R> extends WatermarkAwareProcessor {
    private final SnapshottingCollector<?, B, R> tc;
    private final Map<Long, Map<K, B>> frames = new HashMap<>();

    private CombineFramesP(SnapshottingCollector<?, B, R> tc) {
        this.tc = tc;
    }

    public static <K, B, R> Distributed.Supplier<CombineFramesP<K, B, R>> combineFrames(
            SnapshottingCollector<?, B, R> tc) {
        return () -> new CombineFramesP<>(tc);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final KeyedFrame<K, B> e = (KeyedFrame<K, B>) item;
        final Long frameSeq = e.getSeq();
        final B frame = e.getValue();
        frames.computeIfAbsent(frameSeq, seq -> new HashMap<>())
              .compute(e.getKey(), (s, b) -> {
                          if (b == null) {
                              b = tc.supplier().get();
                          }
                          return tc.combiner().apply(b, frame);
                      });
        return true;
    }

    @Override
    protected boolean tryProcessWm(int ordinal, Watermark wm) {
        FrameClosed frame = (FrameClosed) wm;
        Map<K, B> keys = frames.remove(frame.seq());
        if (keys == null) {
            return true;
        }
        for (Entry<K, B> entry : keys.entrySet()) {
            emit(new KeyedFrame<>(frame.seq(), entry.getKey(), tc.finisher().apply(entry.getValue())));
        }
        return true;
    }
}
