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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.hazelcast.util.Preconditions.checkPositive;
import static java.util.function.Function.identity;

/**
 * Sliding window processor. See {@link
 * WindowingProcessors#slidingWindow(long, long, DistributedCollector)
 * slidingWindow(frameLength, framesPerWindow, collector)} for
 * documentation.
 */
public class SlidingWindowP<K, F, R> extends StreamingProcessorBase {
    private final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
    private final FlatMapper<Punctuation, Object> flatMapper;
    private final BinaryOperator<F> combiner;
    private final Function<F, R> finisher;
    private final Supplier<F> supplier;
    private final long frameLength;
    private final long windowLength;

    private long nextFrameSeqToEmit = Long.MIN_VALUE;

    SlidingWindowP(long frameLength, long framesPerWindow, @Nonnull DistributedCollector<?, F, R> collector) {
        checkPositive(frameLength, "frameLength must be positive");
        checkPositive(framesPerWindow, "framesPerWindow must be positive");
        this.frameLength = frameLength;
        this.windowLength = frameLength * framesPerWindow;
        this.supplier = collector.supplier();
        this.combiner = collector.combiner();
        this.finisher = collector.finisher();
        this.flatMapper = flatMapper(this::slidingWindowTraverser);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Frame<K, F> e = (Frame) item;
        final Long frameSeq = e.getSeq();
        final F frame = e.getValue();
        seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                       .merge(e.getKey(), frame, combiner);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (nextFrameSeqToEmit == Long.MIN_VALUE) {
            if (seqToKeyToFrame.isEmpty()) {
                return true;
            }
            nextFrameSeqToEmit = seqToKeyToFrame.firstKey();
        }
        return flatMapper.tryProcess(punc);
    }

    private Traverser<Object> slidingWindowTraverser(Punctuation punc) {
        return Traversers.<Object>traverseStream(
            range(nextFrameSeqToEmit, updateAndGetNextFrameSeq(punc.seq()), frameLength)
                .mapToObj(frameSeq -> {
                    Map<K, F> window = computeWindow(frameSeq);
                    seqToKeyToFrame.remove(frameSeq - windowLength);
                    return window.entrySet().stream()
                                 .map(e -> new Frame<>(frameSeq, e.getKey(), finisher.apply(e.getValue())));
                }).flatMap(identity()))
            .append(punc);
    }

    private long updateAndGetNextFrameSeq(long puncSeq) {
        long deltaToPunc = puncSeq - nextFrameSeqToEmit;
        if (deltaToPunc < 0) {
            return nextFrameSeqToEmit;
        }
        long frameSeqDelta = deltaToPunc - deltaToPunc % frameLength;
        nextFrameSeqToEmit += frameSeqDelta + frameLength;
        return nextFrameSeqToEmit;
    }

    private Map<K, F> computeWindow(long frameSeq) {
        Map<K, F> window = new HashMap<>();
        Map<Long, Map<K, F>> frames = seqToKeyToFrame.subMap(frameSeq - windowLength, false, frameSeq, true);
        for (Map<K, F> keyToFrame : frames.values()) {
            keyToFrame.forEach((key, currAcc) ->
                    window.compute(key, (x, acc) -> combiner.apply(acc != null ? acc : supplier.get(), currAcc)));
        }
        return window;
    }

    private static LongStream range(long start, long end, long step) {
        return start >= end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start - 1) / step);
    }
}
