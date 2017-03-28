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
 * Collection of punctuation strategies.
 */
public class PunctuationStrategies {

    /**
     * Returns a strategy, which emits punctuations with specified {@code lagMs}.
     * If there are no events for {@code growthDelayMs}, it will start increasing
     * the punctuations at the same rate, as local system time is increasing.
     * <p>
     * If it has never seen any event, we'll start growing the punctuation from
     * {@code Long.MIN_VALUE} after the initial delay. If you cannot
     * <b>guarantee</b>, that all source instances <b>see some events</b>,
     * then using this strategy will cause the processing to practically stop.
     *<p>
     * Event sequence number must be milliseconds.
     *
     * @param lagMs Lag of punctuation behind highest event seq
     * @param growthDelayMs The inactivity period to start growing punctuation
     */
    public static PunctuationStrategy withLagGrowingWithSystemTime(long lagMs, long growthDelayMs) {
        return withLagGrowingWithSystemTime(lagMs, MILLISECONDS.toNanos(growthDelayMs), System::nanoTime);
    }

    static PunctuationStrategy withLagGrowingWithSystemTime(long lagMs, long growthDelayNs, LongSupplier nanoClock) {
        return new PunctuationStrategy() {

            private long lastItemAt = Long.MIN_VALUE;
            private long highestPunctSeq = Long.MIN_VALUE;

            @Override
            public long getPunct(long eventSeq) {
                long now = nanoClock.getAsLong();
                if (eventSeq == Long.MIN_VALUE) {
                    // if we didn't see any item yet, lets suppose the first item has seq of MIN_VALUE and arrived now
                    if (lastItemAt == Long.MIN_VALUE) {
                        lastItemAt = now;
                    }
                    if (now < lastItemAt + growthDelayNs) {
                        return highestPunctSeq;
                    } else {
                        return NANOSECONDS.toMillis(now - lastItemAt - growthDelayNs) + highestPunctSeq;
                    }
                } else {
                    lastItemAt = now;
                    highestPunctSeq = max(highestPunctSeq, eventSeq - lagMs);
                    return highestPunctSeq;
                }
            }
        };
    }

    /**
     * Emits punctuations with lag. No items => no higher punctuation.
     *
     * @param lag The lag. Unit is the same as event sequence unit.
     */
    public static PunctuationStrategy withHighestEventSeqLag(long lag) {
        checkNotNegative(lag, "lag must be >=0");

        return new PunctuationStrategy() {
            private long highestPunc;
            @Override
            public long getPunct(long eventSeq) {
                if (eventSeq == Long.MIN_VALUE) {
                    return highestPunc;
                }
                else {
                    highestPunc = max(highestPunc, eventSeq - lag);
                    return highestPunc;
                }
            }
        };
    }

    /**
     * Emits punctuations with lag with regard to local system time on each node and with
     * regard to highest event sequence, whichever is most ahead.
     * <p>
     * Useful, if {@link #withLagGrowingWithSystemTime(long, long)} cannot be used and
     * we still need the punctuation to grow without data.
     */
    public static PunctuationStrategy withSystemTimeLag(long eventSeqLag, long systemTimeLag) {
        checkNotNegative(eventSeqLag, "eventSeqLag must be >=0");
        checkNotNegative(systemTimeLag, "systemTimeLag must be >=0");

        return new PunctuationStrategy() {
            private long highestPunc;

            @Override
            public long getPunct(long eventSeq) {
                // special case for disabled eventSeqLag
                if (eventSeqLag == Long.MAX_VALUE) {
                    return System.currentTimeMillis() - systemTimeLag;
                }
                highestPunc = max(highestPunc, eventSeq - eventSeqLag);
                return max(eventSeq - eventSeqLag, System.currentTimeMillis() - systemTimeLag);
            }
        };
    }

}
