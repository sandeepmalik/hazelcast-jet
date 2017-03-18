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

package com.hazelcast.jet.impl.util;

import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Helper class to implement the logic needed to enforce the maximum
 * retention time of events in a windowing stream processor. To use this
 * class, call {@link #sample(long, long) sample(currentTime, topEventSeq)}
 * at regular intervals with the current system time and the top observed
 * {@code eventSeq} so far, and interpret the returned value as the minimum
 * value of punctuation that should be/have been emitted. The current time
 * should be obtained from {@code System.nanoTime()} because, unlike
 * {@code System.currentTimeMillis()}, its source is a monotonic clock.
 * <p>
 * This class maintains a circular buffer of samples acquired over the
 * period starting at {@code maxRetain} time units ago and extending to the
 * present. The period is divided into {@code sampleCount} equally-sized
 * intervals and each such interval is mapped to a slot in the buffer.
 * A given {@code sample()} call maps the supplied timestamp to a slot in
 * the buffer, possibly remapping the slots to more recent intervals
 * (thereby automatically discarding the old intervals) and returns the
 * value of the oldest slot after remapping. This slot will be the one
 * that best matches the point in time {@code maxRetain} time units ago.
 * If this class has been used for less than {@code maxRetain} time units,
 * the return value will be {@link Long#MIN_VALUE}.
 * <p>
 * <strong>NOTE:</strong> this class is implemented in terms of some
 * assumptions on the mode of usage:
 * <ol><li>
 *     {@code maxRetain} is expected to be much larger than {@code sampleCount}
 *     and uses an integer size of the sample interval. Therefore the supplied
 *     {@code maxRetain} is rounded down to the nearest multiple of
 *     {@code sampleCount}.
 * </li><li>
 *     {@link #reset(long) reset()} and {@link #sample(long, long) sample()} are
 *     expected to be called with monotonically increasing timestamps and therefore
 *     {@code sample()} always updates the "head" slot, corresponding to the most
 *     recent time interval. If called with a timestamp less than the highest
 *     timestamp used so far, it will still update the head slot.
 * </li></ol>
 */
public class EventSeqHistory {

    private final long[] samples;
    private final long slotInterval;

    private int head;
    private long advanceHeadAt;
    private long prevResult = Long.MIN_VALUE;

    /**
     * @param maxRetain the length of the period over which to keep the
     *                  {@code topEventSeq} history
     * @param sampleCount the number of remembered historical {@code topEventSeq} values
     */
    public EventSeqHistory(long maxRetain, int sampleCount) {
        checkNotNegative(sampleCount - 2, "sampleCount must be at least 2");
        samples = new long[sampleCount];
        slotInterval = maxRetain / sampleCount;
        checkPositive(slotInterval, "maxRetain must be at least equal to sampleCount");
    }

    /**
     * Resets this object's history to {@code Long.MIN_VALUE} and
     * uses the supplied time as the initial point in time.
     */
    public void reset(long now) {
        advanceHeadAt = now + slotInterval;
        Arrays.fill(samples, Long.MIN_VALUE);
        prevResult = Long.MIN_VALUE;
    }

    /**
     * Called to report a new sample along with the timestamp when it was taken.
     * Returns the sample that best matches the point in time {@code maxRetain}
     * units ago, or {@link Long#MIN_VALUE} if sampling started less than
     * {@code maxRetain} time units ago.
     *
     * @param now current time; must not be less than the time used in the previous call
     *            of either this method or {@link #reset(long) reset()}.
     * @param sample the current value of the tracked quantity
     */
    public long sample(long now, long sample) {
        long result = prevResult;
        for (; advanceHeadAt <= now; advanceHeadAt += slotInterval) {
            result = advanceHead();
        }
        samples[head] = sample;
        return prevResult = result;
    }

    private long advanceHead() {
        int nextHead = head + 1 < samples.length ? head + 1 : 0;
        long tailVal = samples[nextHead];
        // Initialize the next head with the previous head value.
        // If sample() wasn't called for longer than the slot interval,
        // this is the best estimate we have for the slot.
        samples[nextHead] = samples[head];
        head = nextHead;
        return tailVal;
    }
}
