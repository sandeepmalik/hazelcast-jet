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
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;

import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

/**
 * Combines frames received from several upstream instances of
 * {@link GroupByFrameP} into finalized frames. Applies the finisher
 * function to produce its emitted output.
 *
 * @param <K> type of grouping key
 * @param <F> type of the frame
 * @param <R> type of the result derived from a frame
 */
public class CombineFramesP<K, F, R> extends FrameProcessorBase<K, F, R> {
    private final BinaryOperator<F> combiner;

    private CombineFramesP(DistributedCollector<?, F, R> collector) {
        super(collector.finisher());
        this.combiner = collector.combiner();
    }

    public static <K, B, R> Distributed.Supplier<CombineFramesP<K, B, R>> combineFrames(
            DistributedCollector<?, B, R> collector
    ) {
        return () -> new CombineFramesP<>(collector);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final KeyedFrame<K, F> e = (KeyedFrame) item;
        final Long frameSeq = e.getSeq();
        final F frame = e.getValue();
        seqToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                  .merge(e.getKey(), frame, combiner);
        return true;
    }
}
