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
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;

import javax.annotation.Nonnull;
import java.util.function.LongSupplier;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Use
 * {@link WindowingProcessors#insertPunctuation(ToLongFunction, Supplier, long, long)
 * WindowingProcessors.insertPunctuation()}.
 *
 * @param <T> input event type
 */
public class InsertPunctuationP<T> extends AbstractProcessor {

    private final ToLongFunction<T> extractEventSeqF;
    private final PunctuationKeeper punctuationKeeper;
    private final long eventSeqThrottle;
    private final long timeThrottle;
    private final LongSupplier clock;

    private long topObservedSeq = Long.MIN_VALUE;
    private long idealPunct = Long.MIN_VALUE;
    private long nextEmissionAtSystemTime = Long.MIN_VALUE;
    private long nextEmissionAtSeq = Long.MIN_VALUE;
    private long lastEmittedPunc = Long.MIN_VALUE;

    /**
     * @param extractEventSeqF function that extracts the {@code eventSeq} from an input item
     * @param punctuationKeeper the punctuation keeper
     * @param eventSeqThrottle the difference between the ideal and the last emitted punctuation
     *                         that triggers the emission of a new punctuation
     * @param timeThrottleMs maximum system time that can pass between emitting successive
     *                       punctuations
     */
    InsertPunctuationP(@Nonnull ToLongFunction<T> extractEventSeqF,
                       @Nonnull PunctuationKeeper punctuationKeeper,
                       long eventSeqThrottle,
                       long timeThrottleMs
    ) {
        this(extractEventSeqF, punctuationKeeper, eventSeqThrottle, MILLISECONDS.toNanos(timeThrottleMs),
                System::nanoTime);
    }

    InsertPunctuationP(@Nonnull ToLongFunction<T> extractEventSeqF,
            @Nonnull PunctuationKeeper punctuationKeeper,
            long eventSeqThrottle,
            long timeThrottle,
            LongSupplier clock
    ) {
        checkNotNegative(eventSeqThrottle, "eventSeqThrottle must be >= 0");
        checkNotNegative(timeThrottle, "timeThrottle must be >= 0");
        this.extractEventSeqF = extractEventSeqF;
        this.punctuationKeeper = punctuationKeeper;
        this.eventSeqThrottle = eventSeqThrottle;
        this.timeThrottle = timeThrottle;
        this.clock = clock;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        T event = (T) item;
        long eventSeq = extractEventSeqF.applyAsLong(event);

        if (eventSeq < idealPunct) {
            // drop late event
            return true;
        }

        // if we see newest item so far, maybe emit punctuation
        if (eventSeq > topObservedSeq) {
            if (!maybeEmitPunctuation(punctuationKeeper.reportEvent(eventSeq))) {
                return false;
            }
            topObservedSeq = eventSeq;
        }
        return tryEmit(item);
    }

    @Override
    public void process() {
        maybeEmitPunctuation(punctuationKeeper.getCurrentPunctuation());
    }

    private boolean maybeEmitPunctuation(long newIdealPunct) {
        // newIdealPunc should be monotonic, but to be sure...
        idealPunct = Math.max(newIdealPunct, idealPunct);

        if (idealPunct <= lastEmittedPunc) {
            return true;
        }

        long now = clock.getAsLong();

        // apply throttling
        if (idealPunct < nextEmissionAtSeq && now < nextEmissionAtSystemTime) {
            return true;
        }

        if (!tryEmit(new Punctuation(idealPunct))) {
            return false;
        }

        // punctuation emitted, let's plan for next emission
        nextEmissionAtSeq = idealPunct + eventSeqThrottle;
        nextEmissionAtSystemTime = now + timeThrottle;
        lastEmittedPunc = idealPunct;
        return true;
    }
}
