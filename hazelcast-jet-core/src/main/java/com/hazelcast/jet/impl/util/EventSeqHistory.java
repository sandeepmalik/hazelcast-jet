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

/**
 * Helper class to implement the logic needed to enforce the maximum
 * retention time of events in a windowing stream processor. To use this
 * class, call {@link #tick(long, long) tick(currentTime, topEventSeq)} at
 * regular intervals with the current system time and the top observed
 * {@code eventSeq} so far, and interpret the returned value as the minimum
 * value of punctuation that should be/have been emitted.
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
    private final long interval;

    private int head = 0;
    private int tail = 1;
    private long nextSlotAt;
    private long lastVal = Long.MIN_VALUE;

    /**
     * @param maxRetain the length of the period over which to keep the
     *                  {@code topEventSeq} history
     * @param numSlots the number of remembered historical {@code topEventSeq} values
     */
    public EventSeqHistory(long maxRetain, int numSlots) {
        slots = new long[numSlots];
        interval = maxRetain / numSlots;
    }

    /**
     * Resets this object's history to {@code Long.MIN_VALUE} and
     * uses the supplied time as the initial point in time.
     */
    public void reset(long now) {
        nextSlotAt = now + interval;
        Arrays.fill(slots, Long.MIN_VALUE);
    }

    /**
     * Returns the {@code topEventSeq} from {@code maxRetain} units ago
     *
     * @param now current system time
     * @param topEventSeq current top event sequence
     */
    public long tick(long now, long topEventSeq) {
        long val = lastVal;
        for (; now >= nextSlotAt; nextSlotAt += interval) {
            val = slide();
        }
        slots[head] = topEventSeq;
        return lastVal = val;
    }

    private long slide() {
        // remember the tail value and advance tail
        long val = slots[tail];
        tail = advance(tail);

        // advance head and copy old head value to new head
        long currHead = slots[head];
        head = advance(head);
        slots[head] = currHead;

        return val;
    }

    private int advance(int index) {
        return index == slots.length - 1 ? 0 : index + 1;
    }
}
