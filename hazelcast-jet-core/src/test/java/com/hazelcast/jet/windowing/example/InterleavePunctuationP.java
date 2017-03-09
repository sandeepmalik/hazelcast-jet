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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A processor to interleave the source with punctuations, when the source does not emit them.
 * <p>
 * The emission is driven by incoming data and its time. If there is no input, the punctuations will
 * stop. New punctuations is emitted for each item, if it is the newest so far.
 * <p>
 * The {@code maxRetain} parameter specifies number of milliseconds (in terms of local system
 * time): if the last emitted punctuation is lower than the highest sequence {@code maxRetain}
 * milliseconds ago,
 *
 * If highest punctuation sequence {@code maxRetain} milliseconds
 *                          ago (in local system time) was higher than the current punctuation,
 *                          it will be emitted as a new punctuation.
 *                          This effectively causes, that if input events stop coming, the punctuation
 *                          after {@code maxRetain} milliseconds will advance to the highest seq value
 *                          seen.
 * <p>
 * Items can be out of order. Items older than the already emitted watermark are not dropped,
 * however you should set the {@link #getPunctuationLag()} to enough time to accomodate for
 * expected out-of-orderness.
 */
public class InterleavePunctuationP<T> extends AbstractProcessor {

    private static final int HISTORIC_SEQS_COUNT = 16;

    private final ToLongFunction<T> extractTimestampF;
    private final long punctuationLag;
    private final LongSupplier clock;
    private final long maxRetain;
    private long highestSeq = Long.MIN_VALUE;
    private long[] historicSeqs;
    private int historicSeqsPos;
    private long historyInterval;
    private long nextHistoryFrameStart;
    private long highestPunctuation;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        nextHistoryFrameStart = clock.getAsLong() + historyInterval;
    }

    /**
     * Time unit for {@code punctuationLag} and {@code punctuationInterval} is in the same
     * units as {@code extractTimestampF}.
     *
     * @param extractTimestampF Function to extract timestamp from input items
     * @param punctuationLag    Time the punctuation is behind the most recent event
     * @param maxRetain         See {@link InterleavePunctuationP}
     */
    public InterleavePunctuationP(ToLongFunction<T> extractTimestampF, long punctuationLag,
            long maxRetain) {
        this(extractTimestampF, punctuationLag, MILLISECONDS.toNanos(maxRetain), System::nanoTime);
    }

    InterleavePunctuationP(ToLongFunction<T> extractTimestampF, long punctuationLag,
            long maxRetain, LongSupplier clock) {
        this.extractTimestampF = extractTimestampF;
        this.punctuationLag = punctuationLag;
        this.clock = clock;

        historicSeqs = new long[HISTORIC_SEQS_COUNT];
        Arrays.fill(historicSeqs, Long.MIN_VALUE);

        historyInterval = maxRetain / HISTORIC_SEQS_COUNT;
        this.maxRetain = historyInterval * HISTORIC_SEQS_COUNT;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        emit(item);
        T tItem = (T) item;
        long itemSeq = extractTimestampF.applyAsLong(tItem);

        // if sufficient time passed since the last punctuation, emit a new one
        if (itemSeq > highestSeq) {
            highestSeq = itemSeq;
            maybeEmitPunctuation(highestSeq - punctuationLag);
        }

        return true;
    }

    @Override
    public void process() {
        long now = clock.getAsLong();

        if (nextHistoryFrameStart <= now) {
            long punctToEmit = Long.MIN_VALUE;
            while (nextHistoryFrameStart <= now) {
                nextHistoryFrameStart += historyInterval;
                historicSeqsPos++;
                if (historicSeqsPos == HISTORIC_SEQS_COUNT) {
                    historicSeqsPos = 0;
                }
                punctToEmit = historicSeqs[historicSeqsPos];

                // initialize the current bucket to current max. If this is an old bucket, initialize it to current-lag
                historicSeqs[historicSeqsPos] = highestSeq - (nextHistoryFrameStart < now ? punctuationLag : 0);
            }

            // emit the punctuation
            maybeEmitPunctuation(punctToEmit);
        }
    }

    private void maybeEmitPunctuation(long at) {
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
