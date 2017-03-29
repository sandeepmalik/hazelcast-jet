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

import com.hazelcast.jet.Distributed.LongSupplier;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility class with several punctuation strategies.
 */
public final class PunctuationStrategies {

    private PunctuationStrategies() {
    }

    /**
     * This strategy returns punctuation that lags behind the top observed
     * event seq by the given amount. In the case of a stream lull the
     * punctuation does not advance.
     *
     * @param puncLag the desired difference between the top observed event seq
     *                and the punctuation
     */
    public static PunctuationStrategy cappingEventSeqLag(long puncLag) {
        checkNotNegative(puncLag, "puncLag must not be negative");

        return new PunctuationStrategy() {
            private long punc = Long.MIN_VALUE;

            @Override
            public long getPunctuation(long eventSeq) {
                if (eventSeq == Long.MIN_VALUE) {
                    return punc;
                }
                punc = max(punc, eventSeq - puncLag);
                return punc;
            }
        };
    }

    /**
     * This strategy returns punctuation that lags behind the top event seq by
     * at most {@code eventSeqLag} and behind wall-clock time by at most
     * {@code systemTimeLag}. It assumes that {@code eventSeq} is the timestamp
     * of the event in milliseconds and will use that fact to correlate
     * {@code eventSeq} with wall-clock time acquired from the underlying OS.
     * Note that wall-clock time is non-monotonic and when sudden jumps occur
     * in it, this will cause temporary disruptions in the functioning of this
     * strategy.
     * <p>
     * In most cases the {@link #cappingEventSeqLagAndLull(long, long)
     * cappingEventSeqLagAndLull} strategy should be preferred; this is a
     * backup option for cases where some substreams may never see an event.
     */
    public static PunctuationStrategy cappingEventSeqAndWallClockLag(long eventSeqLag, long systemTimeLag) {
        checkNotNegative(eventSeqLag, "eventSeqLag must be >=0");
        checkNotNegative(systemTimeLag, "systemTimeLag must be >=0");

        return new PunctuationStrategy() {
            private long highestPunc;

            @Override
            public long getPunctuation(long eventSeq) {
                // special case for disabled eventSeqLag
                if (eventSeqLag == Long.MAX_VALUE) {
                    return System.currentTimeMillis() - systemTimeLag;
                }
                highestPunc = max(highestPunc, eventSeq - eventSeqLag);
                return max(eventSeq - eventSeqLag, System.currentTimeMillis() - systemTimeLag);
            }
        };
    }

    /**
     * This strategy returns punctuation that lags behind the top event seq by
     * the amount specified with {@code puncLag}. The strategy assumes that
     * event seq corresponds to the timestamp of the event given in milliseconds
     * and will use that fact to correlate the event seq with the passage of
     * system time.
     * <p>
     * When the defined {@code maxLullMs} period elapses without observing more
     * events, the punctuation will start advancing in lockstep with system
     * time acquired from the underlying OS's monotonic clock.
     * <p>
     * If no event is ever observed, the punctuation will advance from the initial
     * value of {@code Long.MIN_VALUE}. Therefore this strategy can be used only
     * when there is a guarantee that each substream will emit at least some events
     * to initialize the {@code eventSeq}. Otherwise the empty substream will hold
     * back the processing of all other substreams by keeping the punctuation below
     * any realistic value.
     *
     * @param puncLag lag of punctuation behind the highest event seq
     * @param maxLullMs maximum duration of a lull period before starting to
     *                  advance punctuation with system time
     */
    public static PunctuationStrategy cappingEventSeqLagAndLull(long puncLag, long maxLullMs) {
        return cappingEventSeqLagAndLull(puncLag, MILLISECONDS.toNanos(maxLullMs), System::nanoTime);
    }

    static PunctuationStrategy cappingEventSeqLagAndLull(long seqLag, long maxLullNs, LongSupplier nanoClock) {
        return new PunctuationStrategy() {

            private long lastEventAt = Long.MIN_VALUE;
            private long topPuncSeq = Long.MIN_VALUE;

            @Override
            public long getPunctuation(long eventSeq) {
                long now = nanoClock.getAsLong();
                if (eventSeq != Long.MIN_VALUE) {
                    lastEventAt = now;
                    topPuncSeq = max(topPuncSeq, eventSeq - seqLag);
                    return topPuncSeq;
                }
                if (lastEventAt == Long.MIN_VALUE) {
                    // if we haven't seen any events yet, we behave as if
                    // the first event had seq = MIN_VALUE and arrived now
                    lastEventAt = now;
                }
                long nanosSinceLastEvent = now - lastEventAt;
                long addedSeq = max(0, nanosSinceLastEvent - maxLullNs);
                return topPuncSeq + NANOSECONDS.toMillis(addedSeq);
            }
        };
    }
}
