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
import com.hazelcast.jet.Distributed.LongUnaryOperator;
import com.hazelcast.jet.Distributed.ToLongFunction;

import javax.annotation.Nonnull;

/**
 * <ol><li>
 *     Extracts the timestamp from the received item.
 * </li><li>
 *     Determines the frame seq from the timestamp.
 * </li><li>
 *     If the frame seq is more recent than the last emitted punctuation plus the
 *     number of allowed open frames, emits all the punctuations needed to catch
 *     up with the frame seq.
 * </li></ol>
 */
public class PunctuationStageOneP<T> extends AbstractProcessor {

    private final ToLongFunction<? super T> extractTimestampF;
    private final LongUnaryOperator toFrameSeqF;
    private final int openFrameCount;

    private long nextPuncSeq = Long.MIN_VALUE;

    private PunctuationStageOneP(
            ToLongFunction<? super T> extractTimestampF, LongUnaryOperator toFrameSeqF, int openFrameCount
    ) {
        this.extractTimestampF = extractTimestampF;
        this.toFrameSeqF = toFrameSeqF;
        this.openFrameCount = openFrameCount;
    }

    public static <T> Distributed.Supplier<PunctuationStageOneP> punctuationStageOne(
            ToLongFunction<? super T> extractTimestampF,
            LongUnaryOperator toFrameSeqF,
            int openFrameCount
    ) {
        return () -> new PunctuationStageOneP(extractTimestampF, toFrameSeqF, openFrameCount);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        T t = (T) item;
        final long itemFrameSeq = toFrameSeqF.applyAsLong(extractTimestampF.applyAsLong(t));
        if (nextPuncSeq == Long.MIN_VALUE) {
            nextPuncSeq = itemFrameSeq - (openFrameCount - 1);
        }
        while (nextPuncSeq + openFrameCount <= itemFrameSeq) {
            emit(new SeqPunctuation(++nextPuncSeq));
        }
        emit(t);
        return true;
    }
}
