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
import com.hazelcast.jet.impl.util.EventSeqHistory;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility class with several punctuation keepers.
 */
public final class PunctuationKeepers {

    private PunctuationKeepers() {
    }

    private abstract static class PunctuationKeeperBase implements PunctuationKeeper {

        private long punc = Long.MIN_VALUE;

        long makePuncAtLeast(long proposedPunc) {
            punc = max(punc, proposedPunc);
            return punc;
        }

        long advancePuncBy(long amount) {
            punc += amount;
            return punc;
        }

        @Override
        public long getCurrentPunctuation() {
            return punc;
        }
    }

    /**
     * Maintains punctuation that lags behind the top observed event seq by the
     * given amount. In the case of a stream lull the punctuation does not
     * advance and stays lagged.
     *
     * @param eventSeqLag the desired difference between the top observed event seq
     *                    and the punctuation
     */
    public static PunctuationKeeper cappingEventSeqLag(long eventSeqLag) {
        checkNotNegative(eventSeqLag, "eventSeqLag must not be negative");

        return new PunctuationKeeperBase() {

            @Override
            public long reportEvent(long eventSeq) {
                return makePuncAtLeast(eventSeq - eventSeqLag);
            }
        };
    }

    /**
     * Maintains punctuation that lags behind the top observed event seq by the
     * given amount. Additionally, the punctuation will eventually
     * advance to the top observed sequence within a maximum of {@code maxRetainMs} milliseconds.
     *
     * @param eventSeqLag the desired difference between the top observed event seq
     *                    and the punctuation
     * @param maxRetainMs upper bound on how long system time it will take for the punctuation to eventually
     *                    reach the highest observed sequence.
     *
     */
    public static PunctuationKeeper cappingEventSeqLagAndRetention(long eventSeqLag, long maxRetainMs) {
        return cappingEventSeqLagAndRetention(eventSeqLag, MILLISECONDS.toNanos(maxRetainMs), 16, System::nanoTime);
    }


    static PunctuationKeeper cappingEventSeqLagAndRetention(long eventSeqLag, long maxRetainNanos, int numStoredSamples,
                                                            LongSupplier nanoClock) {
        return new PunctuationKeeperBase() {

            private long topSeq = Long.MIN_VALUE;
            private EventSeqHistory history = new EventSeqHistory(maxRetainNanos, numStoredSamples);

            @Override
            public long reportEvent(long eventSeq) {
                topSeq = Math.max(eventSeq, topSeq);
                long proposedPunc = Math.max(history.sample(nanoClock.getAsLong(), topSeq), eventSeq - eventSeqLag);
                return makePuncAtLeast(proposedPunc);
            }

            @Override
            public long getCurrentPunctuation() {
                long proposedPunc = Math.max(history.sample(nanoClock.getAsLong(), topSeq), super.getCurrentPunctuation());
                return makePuncAtLeast(proposedPunc);
            }
        };
    }

    /**
     * Maintains punctuation that lags behind the top event seq by at most
     * {@code eventSeqLag} and behind wall-clock time by at most
     * {@code wallClockLag}. It assumes that {@code eventSeq} is the timestamp
     * of the event in milliseconds since Unix epoch and will use that fact to
     * correlate {@code eventSeq} with wall-clock time acquired from the
     * underlying OS. Note that wall-clock time is non-monotonic and sudden
     * jumps that may occur in it will cause temporary disruptions in the
     * functioning of this punctuation keeper.
     * <p>
     * In most cases the {@link #cappingEventSeqLagAndLull(long, long)
     * cappingEventSeqLagAndLull} keeper should be preferred; this is a
     * backup option for cases where some substreams may never see an event.
     *
     * @param eventSeqLag maximum difference between the top observed event seq
     *                    and the punctuation
     * @param wallClockLag maximum difference between the current value of
     *                     {@code System.currentTimeMillis} and the punctuation
     */
    public static PunctuationKeeper cappingEventSeqAndWallClockLag(long eventSeqLag, long wallClockLag) {
        checkNotNegative(eventSeqLag, "eventSeqLag must not be negative");
        checkNotNegative(wallClockLag, "wallClockLag must not be negative");

        return new PunctuationKeeperBase() {

            @Override
            public long reportEvent(long eventSeq) {
                updateFromWallClock();
                return makePuncAtLeast(eventSeq - eventSeqLag);
            }

            @Override
            public long getCurrentPunctuation() {
                return updateFromWallClock();
            }

            private long updateFromWallClock() {
                return makePuncAtLeast(System.currentTimeMillis() - wallClockLag);
            }
        };
    }

    /**
     * Maintains punctuation that lags behind the top event seq by the amount
     * specified with {@code eventSeqLag}. Assumes that event seq corresponds
     * to the timestamp of the event given in milliseconds and will use that
     * fact to correlate the event seq with the passage of system time. There
     * is no requirement on any specific point of origin for the event time,
     * i.e., the zero value can denote any point in time as long as it is fixed.
     * <p>
     * When the defined {@code maxLullMs} period elapses without observing more
     * events, punctuation will start advancing in lockstep with system time
     * acquired from the underlying OS's monotonic clock.
     * <p>
     * If no event is ever observed, punctuation will advance from the initial
     * value of {@code Long.MIN_VALUE}. Therefore this keeper can be used only
     * when there is a guarantee that each substream will emit at least one
     * event that will initialize the {@code eventSeq}. Otherwise the empty
     * substream will hold back the processing of all other substreams by
     * keeping the punctuation below any realistic value.
     *
     * @param eventSeqLag the desired difference between the top observed event seq
     *                    and the punctuation
     * @param maxLullMs maximum duration of a lull period before starting to
     *                  advance punctuation with system time
     */
    public static PunctuationKeeper cappingEventSeqLagAndLull(long eventSeqLag, long maxLullMs) {
        return cappingEventSeqLagAndLull(eventSeqLag, maxLullMs, System::nanoTime);
    }


    static PunctuationKeeper cappingEventSeqLagAndLull(long eventSeqLag, long maxLullMs, LongSupplier
            nanoClock) {
        checkNotNegative(eventSeqLag, "eventSeqLag must not be negative");
        checkNotNegative(maxLullMs, "maxLullMs must not be negative");

        return new PunctuationKeeperBase() {

            private long maxLullAt = Long.MIN_VALUE;

            @Override
            public long reportEvent(long eventSeq) {
                maxLullAt = monotonicTimeMillis() + maxLullMs;
                return makePuncAtLeast(eventSeq - eventSeqLag);
            }

            @Override
            public long getCurrentPunctuation() {
                long now = monotonicTimeMillis();
                ensureInitialized(now);
                long millisPastMaxLull = max(0, now - maxLullAt);
                maxLullAt += millisPastMaxLull;
                return advancePuncBy(millisPastMaxLull);
            }

            private void ensureInitialized(long now) {
                if (maxLullAt == Long.MIN_VALUE) {
                    maxLullAt = now + maxLullMs;
                }
            }

            private long monotonicTimeMillis() {
                return NANOSECONDS.toMillis(nanoClock.getAsLong());
            }
        };
    }
}
