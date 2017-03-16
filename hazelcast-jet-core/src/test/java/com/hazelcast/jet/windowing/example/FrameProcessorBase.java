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
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.jet.Traverser.concat;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseIterableWithRemoval;

/**
 * Base class with logic common to {@link GroupByFrameP} and
 * {@link CombineFramesP}.
 */
public abstract class FrameProcessorBase<K, F, R> extends StreamingProcessorBase {
    final SortedMap<Long, Map<K, F>> seqToFrame = new TreeMap<>();
    private final FlatMapper<Punctuation, Object> puncFlatMapper;

    FrameProcessorBase(Function<F, R> finisher) {
        this.puncFlatMapper = flatMapper((Punctuation punc) ->
                traverseIterableWithRemoval(seqToFrame.headMap(punc.seq() + 1).entrySet())
                        .flatMap(seqAndFrame -> concat(
                                traverseIterable(seqAndFrame.getValue().entrySet())
                                        .map(e -> new KeyedFrame<>(
                                                seqAndFrame.getKey(), e.getKey(), finisher.apply(e.getValue()))),
                                Traverser.over(new Punctuation(seqAndFrame.getKey())))));
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        return puncFlatMapper.tryProcess(punc);
    }
}
