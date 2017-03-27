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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.EventSeqHistory;

import javax.annotation.Nonnull;
import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A processor that inserts punctuation into a data stream. Punctuation is
 * determined as the top observed {@code eventSeq} minus the configured
 * {@code punctuationLag}. We shall say that a punctuation is <em>behind</em>
 * an item if the item's {@code eventSeq} is higher than it; we shall use
 * the term <em>ahead</em> for the opposite case.
 * <p>
 * Since eagerly emitting punctuation every time a new top {@code eventSeq}
 * is observed would cause too much overhead, there is throttling that skips
 * some opportunities to emit punctuation. We shall therefore distinguish
 * between the <em>ideal punctuation</em> and the <em>emitted punctuation</em>.
 * There are two triggers that will cause a new punctuation to be emitted:
 * <ol><li>
 *     The difference between the ideal and the last emitted punctuation: when it
 *     exceeds the configured {@code eventSeqTrigger}, a new punctuation is
 *     emitted.
 * </li><li>
 *     The difference between the current time and the time the last punctuation
 *     was emitted: when it exceeds the configured {@code timeTrigger}, and if the
 *     current ideal punctuation is greater than the emitted punctuation, a new
 *     punctuation will be emitted.
 * </li></ol>
 * If the top {@code eventSeq} observed {@code maxRetainTime} milliseconds
 * ago was higher than the last emitted punctuation, a new punctuation will
 * be emitted with that {@code eventSeq}. This limits the amount of time the
 * emitted punctuation can stay behind any observed event. Since punctuation
 * drives the emission and removal of aggregated state from processors, this
 * is also the maximum time to retain data about an event in the system before
 * sending it to the data sink.
 *
 * @param <T> the type of stream item
 */
public class InsertPunctuationP<T> extends AbstractProcessor {

    private static final int HISTORIC_SEQS_COUNT = 16;

    private final ToLongFunction<T> extractEventSeqF;
    private final long punctuationLag;
    private final long eventSeqTrigger;
    private final long timeTrigger;
    private final LongSupplier clock;
    private final EventSeqHistory eventSeqHistory;

    private long highestSeq = Long.MIN_VALUE;
    private long lastEmittedPunc;

    private long nextPuncEventSeq;
    private long nextPuncTime;

    /**
     * @param extractEventSeqF function that extracts the {@code eventSeq} from an input item
     * @param punctuationLag   the difference between the top observed {@code eventSeq} and
     *                         the ideal punctuation
     * @param maxRetainTime    maximum time the emitted punctuation can stay behind any observed
     *                         event
     * @param eventSeqTrigger the difference between the ideal and the last emitted punctuation
     *                        that triggers the emission of a new punctuation
     * @param timeTrigger maximum time that can pass between emitting successive punctuations
     */
    public InsertPunctuationP(ToLongFunction<T> extractEventSeqF, long punctuationLag,
                              long maxRetainTime, long eventSeqTrigger, long timeTrigger) {
        this(extractEventSeqF, punctuationLag, MILLISECONDS.toNanos(maxRetainTime),
                eventSeqTrigger, MILLISECONDS.toNanos(timeTrigger), System::nanoTime);
    }

    InsertPunctuationP(ToLongFunction<T> extractEventSeqF, long punctuationLag,
                       long maxRetain, long eventSeqTrigger, long timeTrigger, LongSupplier clock) {
        this.extractEventSeqF = extractEventSeqF;
        this.punctuationLag = punctuationLag;
        this.clock = clock;
        this.eventSeqTrigger = eventSeqTrigger;
        this.timeTrigger = timeTrigger;

        this.eventSeqHistory = new EventSeqHistory(maxRetain, HISTORIC_SEQS_COUNT);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        T tItem = (T) item;
        long itemSeq = extractEventSeqF.applyAsLong(tItem);

        // if we have newest item so far, maybe emit punctuation
        if (itemSeq > highestSeq) {
            highestSeq = itemSeq;
            maybeEmitPunctuation(highestSeq - punctuationLag);
        }
        emit(item);

        return true;
    }

    @Override
    public void process() {
        maybeEmitPunctuation(eventSeqHistory.sample(clock.getAsLong(), highestSeq));
    }

    private void maybeEmitPunctuation(long punctuationTime) {
        long now = clock.getAsLong();
        if (lastEmittedPunc < punctuationTime &&
                (punctuationTime >= nextPuncEventSeq || now >= nextPuncTime)) {
            emit(new Punctuation(punctuationTime));
            lastEmittedPunc = punctuationTime;
            nextPuncEventSeq = punctuationTime + eventSeqTrigger;
            nextPuncTime = now + timeTrigger;
        }
    }
}
