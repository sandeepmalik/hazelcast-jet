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
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * A processor to interleave the source with punctuations, when the source does not emit any
 * punctuation.
 * <p>
 * The emission is driven by incoming data and its time. If there is no input, there is no
 * punctuation. New punctuations is emitted, whenever an item newer than
 * {@code lastPunctuation + }{@link #getPunctuationInterval()} is emitted.
 * <p>
 * Items can be out of order. Items older than the already emitted watermark are not dropped,
 * you should set the {@link #getPunctuationLag()} to accommodate for enough time for
 * expected out-of-orderness.
 */
public class InterleavePunctuationP<T> extends AbstractProcessor {

    private static final int HISTORIC_SEQS_COUNT = 16;

    private final ToLongFunction<T> extractTimestampF;
    private final long punctuationLag;
    private final LongSupplier clock;
    private long highestSeq = Long.MIN_VALUE;
    private long[] historicSeqs;
    private int historicSeqsPos;
    private long historyInterval;
    private long currentHistoryFrameEnd;
    private long highestPunctuation;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        currentHistoryFrameEnd = clock.getAsLong() + historyInterval;
    }

    /**
     * Time unit for {@code punctuationLag} and {@code punctuationInterval} is in the same
     * units as {@code extractTimestampF}.
     *
     * @param extractTimestampF   Function to extract timestamp from input items
     * @param punctuationLag      Time the punctuation is behind the most recent event
     * @param punctuationInterval Minimum time between subsequent punctuations.
     */
    public InterleavePunctuationP(ToLongFunction<T> extractTimestampF, long punctuationLag,
            long maxRetain) {
        this(extractTimestampF, punctuationLag, maxRetain, System::nanoTime);
    }

    InterleavePunctuationP(ToLongFunction<T> extractTimestampF, long punctuationLag,
            long maxRetain, LongSupplier clock) {
        this.extractTimestampF = extractTimestampF;
        this.punctuationLag = punctuationLag;
        this.clock = clock;

        historicSeqs = new long[HISTORIC_SEQS_COUNT];
        Arrays.fill(historicSeqs, Long.MIN_VALUE);

        historyInterval = maxRetain / HISTORIC_SEQS_COUNT;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        emit(item);
        T tItem = (T) item;
        long itemSeq = extractTimestampF.applyAsLong(tItem);

        // if sufficient time passed since the last punctuation, emit a new one
        if (itemSeq > highestSeq) {
            highestSeq = itemSeq;
            emitPunctuation(highestSeq - punctuationLag);
        }

        return true;
    }

    @Override
    public void process() {
        long now = clock.getAsLong();

        while (currentHistoryFrameEnd < now) {
            long nextFrameEnd = currentHistoryFrameEnd + historyInterval;

            int oldestPos = historicSeqsPos + 1 == HISTORIC_SEQS_COUNT ? 0 : historicSeqsPos + 1;

            // emit the oldest bucket, if it is newer than current max
            if (historicSeqs[oldestPos] > highestSeq - punctuationLag) {
                emitPunctuation(historicSeqs[oldestPos]);
            }

            historicSeqsPos = oldestPos;

            // initialize the current bucket to current max. If this is an old bucket, initialize it to current-lag
            historicSeqs[historicSeqsPos] = highestSeq - (nextFrameEnd < now ? punctuationLag : 0);

            currentHistoryFrameEnd += historyInterval;
        }
    }

    private void emitPunctuation(long at) {
        if (at > highestPunctuation) {
            highestPunctuation = at;
            emit(new Punctuation(at));
        }
    }

    @Override
    public boolean tryProcessPunctuation(int ordinal, Punctuation punc) {
        getLogger().warning("Received unexpected punctuation:" + punc);
        return true;
    }

    public long getPunctuationLag() {
        return punctuationLag;
    }
}
