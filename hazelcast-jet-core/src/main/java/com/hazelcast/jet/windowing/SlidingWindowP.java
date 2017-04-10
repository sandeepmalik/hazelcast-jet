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

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.StreamingProcessorBase;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static java.util.function.Function.identity;

/**
 * Sliding window processor. See {@link
 * WindowingProcessors#slidingWindow(long, long, WindowMaker)
 * slidingWindow(frameLength, framesPerWindow, windowMaker)} for
 * documentation.
 */
public class SlidingWindowP<K, F, R> extends StreamingProcessorBase {
    private final long frameLength;
    private final long windowLength;
    private final Supplier<F> createF;
    private final BinaryOperator<F> combineF;
    private final Distributed.BinaryOperator<F> deductF;
    private final Function<F, R> finishF;
    private final FlatMapper<Punctuation, Object> flatMapper;
    private final F emptyAcc;
    private final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
    private final Map<K, F> slidingWindow = new HashMap<>();

    private long nextFrameSeqToEmit = Long.MIN_VALUE;

    SlidingWindowP(WindowDefinition wdef, @Nonnull WindowMaker<K, F, R> windowMaker) {
        this.frameLength = wdef.frameLength();
        this.windowLength = wdef.windowLength();
        this.createF = windowMaker.createAccumulatorF();
        this.combineF = windowMaker.combineAccumulatorsF();
        this.deductF = windowMaker.combineAccumulatorsF();
        this.finishF = windowMaker.finishAccumulationF();
        this.flatMapper = flatMapper(deductF != null ? this::slideByDiffing : this::slideByRecomputing);
        this.emptyAcc = createF.get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Frame<K, F> e = (Frame) item;
        final Long frameSeq = e.getSeq();
        final F frame = e.getValue();
        seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                       .merge(e.getKey(), frame, combineF);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (nextFrameSeqToEmit == Long.MIN_VALUE) {
            // This is the first punctuation we are observing. Use the lowest frame of
            // the window that ends at punc seq as the first frame to be emitted and
            // delete all frames that precede it. The rest of the frames will be
            // gradually evicted as the window slides along. The above guarantees that
            // the sliding window can be correctly initialized using the "accumulate
            // highest/deduct lowest" approach because we start from a window that
            // covers at most one existing frame -- the lowest one that can remain
            // after the deletion happening here.
            nextFrameSeqToEmit = punc.seq() - windowLength;
            seqToKeyToFrame.headMap(nextFrameSeqToEmit).clear();
        }
        return flatMapper.tryProcess(punc);
    }

    private Traverser<Object> slideByRecomputing(Punctuation punc) {
        return Traversers.<Object>traverseStream(
            range(nextFrameSeqToEmit, updateAndGetNextFrameSeq(punc.seq()), frameLength)
                .mapToObj(frameSeq -> {
                    Map<K, F> window = computeWindow(frameSeq);
                    seqToKeyToFrame.remove(frameSeq - windowLength);
                    return window.entrySet().stream()
                                 .map(e -> new Frame<>(frameSeq, e.getKey(), finishF.apply(e.getValue())));
                }).flatMap(identity()))
            .append(punc);
    }

    private Traverser<Object> slideByDiffing(Punctuation punc) {
        return Traversers.<Object>traverseStream(
                range(nextFrameSeqToEmit, updateAndGetNextFrameSeq(punc.seq()), frameLength)
                        .mapToObj(frameSeq -> {
                            applyPatch(combineF, seqToKeyToFrame.get(frameSeq), slidingWindow);
                            applyPatch(deductF, seqToKeyToFrame.remove(frameSeq - windowLength), slidingWindow);
                            return slidingWindow
                                    .entrySet().stream()
                                    .map(e -> new Frame<>(frameSeq, e.getKey(), finishF.apply(e.getValue())));
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

    private void applyPatch(BinaryOperator<F> patchOp, Map<K, F> patchingFrame, Map<K, F> patchTarget) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<K, F> e : patchingFrame.entrySet()) {
            patchTarget.compute(e.getKey(), (k, acc) -> {
                F result = patchOp.apply(acc, e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    private Map<K, F> computeWindow(long frameSeq) {
        Map<K, F> window = new HashMap<>();
        Map<Long, Map<K, F>> frames = seqToKeyToFrame.subMap(frameSeq - windowLength, false, frameSeq, true);
        for (Map<K, F> keyToFrame : frames.values()) {
            keyToFrame.forEach((key, currAcc) ->
                    window.compute(key, (x, acc) -> combineF.apply(acc != null ? acc : createF.get(), currAcc)));
        }
        return window;
    }

    private static LongStream range(long start, long end, long step) {
        return start >= end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start - 1) / step);
    }
}
