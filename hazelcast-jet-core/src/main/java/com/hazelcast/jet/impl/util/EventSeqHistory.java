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
 * This class maintains an array of {@code topEventSeq} values observed at
 * evenly spaced points in time over the period extending from
 * {@code maxRetain} time units ago till the present. The size of the array
 * is configured by the {@code numSlots} constructor parameter. A given
 * {@code tick()} call will get the oldest remembered value, which
 * corresponds to the point in time best matching the ideal point exactly
 * {@code maxRetain} time units ago. If this class has been used for less
 * than {@code maxRetain} time units, the return value will be
 * {@link Long#MIN_VALUE}.
 */
public class EventSeqHistory {

    private final long[] slots;
    private final long slotInterval;

    private int head;
    private int tail = 1;
    private long prevResult = Long.MIN_VALUE;
    private long gotoNextSlotAt;

    /**
     * @param maxRetain the length of the period over which to keep the
     *                  {@code topEventSeq} history
     * @param numSlots the number of remembered historical {@code topEventSeq} values
     */
    public EventSeqHistory(long maxRetain, int numSlots) {
        checkNotNegative(numSlots - 2, "Number of slots must be at least 2");
        slots = new long[numSlots];
        slotInterval = maxRetain / numSlots;
        checkPositive(slotInterval, "maxRetain must be at least equal to numSlots");
    }

    /**
     * Resets this object's history to {@code Long.MIN_VALUE} and
     * uses the supplied time as the initial point in time.
     */
    public void reset(long now) {
        gotoNextSlotAt = now + slotInterval;
        Arrays.fill(slots, Long.MIN_VALUE);
        prevResult = Long.MIN_VALUE;
    }

    /**
     * Called to report a new {@code topEventSeq} sample along with the timestamp
     * when it was taken. Returns the sample from {@code maxRetain} time units
     * ago, or {@link Long#MIN_VALUE} if sampling started less than
     * {@code maxRetain} time units ago.
     *
     * @param now current time
     * @param sample current top event sequence
     */
    public long sample(long now, long sample) {
        long result = prevResult;
        for (; gotoNextSlotAt <= now; gotoNextSlotAt += slotInterval) {
            result = slide();
        }
        slots[head] = sample;
        return prevResult = result;
    }

    private long slide() {
        long tailVal = slots[tail];
        tail = advance(tail);

        long headVal = slots[head];
        head = advance(head);
        // Initialize the new head with the previous head value.
        // If sample() wasn't called for longer than the slot interval,
        // this is the best estimate we have for the slot.
        slots[head] = headVal;

        return tailVal;
    }

    private int advance(int index) {
        return index + 1 < slots.length ? index + 1 : 0;
    }
}
