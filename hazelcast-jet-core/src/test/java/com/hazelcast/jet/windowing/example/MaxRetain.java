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

import java.util.Arrays;

public class MaxRetain {

    private long[] seqs;
    private int head = 0;
    private int tail = 1;
    private long interval;
    private long nextSlotAt;
    private long lastVal;

    MaxRetain(int count, long duration) {
        seqs = new long[count];
        Arrays.fill(seqs, Long.MIN_VALUE);
        interval = duration / count;
    }

    public void init(long now) {
        nextSlotAt = now + interval;
    }

    long tick(long now, long topSeq) {
        long val = lastVal;
        for (; now >= nextSlotAt; nextSlotAt += interval) {
            val = slide();
        }
        seqs[head] = topSeq;
        return (lastVal = val);
    }

    private long slide() {
        // advance tail and keep value of current tail
        long val = seqs[tail];
        tail = advance(tail);

        // advance head and copy value to new head
        long currHead = seqs[head];
        head = advance(head);
        seqs[head] = currHead;

        return val;
    }

    private int advance(int index) {
        if (++index == seqs.length) {
            return 0;
        }
        return index;
    }
}
