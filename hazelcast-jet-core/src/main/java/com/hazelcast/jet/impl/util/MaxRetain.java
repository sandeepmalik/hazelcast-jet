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
 * Helper class for maxRetain calculation.
 *
 * maxRetain is defined as maximum system time that can pass between
 * observing an event with the top eventSeq and emitting a
 * punctuation with that value.
 */
public class MaxRetain {

    private final long[] slots;
    private final long interval;

    private int head = 0;
    private int tail = 1;
    private long nextSlotAt;
    private long lastVal = Long.MIN_VALUE;

    /**
     *
     * @param maxRetain the maximum duration a top value can be retained
     * @param numSlots how many slots to divide the duration into
     */
    public MaxRetain(long maxRetain, int numSlots) {
        if (numSlots < 2)
            throw new IllegalArgumentException("numSlots must be >=2");
        slots = new long[numSlots];
        interval = maxRetain / numSlots;
        if (interval < 0)
            throw new IllegalArgumentException("maxRetain must be larger than numSlots");
    }

    /**
     * Reset to the initial time value.
     */
    public void reset(long now) {
        nextSlotAt = now + interval;
        Arrays.fill(slots, Long.MIN_VALUE);
    }

    /**
     *
     * @param now current system time
     * @param topSeq current top sequence
     * @return the top sequence from {@code maxRetain} units ago
     */
    public long tick(long now, long topSeq) {
        long val = lastVal;
        for (; now >= nextSlotAt; nextSlotAt += interval) {
            val = slide();
        }
        slots[head] = topSeq;
        return (lastVal = val);
    }

    private long slide() {
        // advance tail and keep value of current tail
        long val = slots[tail];
        tail = advance(tail);

        // advance head and copy value to new head
        long currHead = slots[head];
        head = advance(head);
        slots[head] = currHead;

        return val;
    }

    private int advance(int index) {
        if (++index == slots.length) {
            return 0;
        }
        return index;
    }
}
