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

import com.hazelcast.jet.impl.util.EventSeqHistory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventSeqHistoryTest {

    private EventSeqHistory histo;

    @Before
    public void setup() {
        histo = new EventSeqHistory(4, 4);
        histo.reset(0);
    }

    @Test
    public void when_size1_then_works() {
        histo = new EventSeqHistory(2, 1);
        histo.reset(0);
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(Long.MIN_VALUE, tick(2, 2));
        assertEquals(1, tick(3, 3));
    }

    @Test
    public void when_clockIncreasingByOne() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(Long.MIN_VALUE, tick(2, 2));
        assertEquals(Long.MIN_VALUE, tick(3, 3));
        assertEquals(Long.MIN_VALUE, tick(4, 4));
        assertEquals(1, tick(5, 5));
        assertEquals(2, tick(6, 6));
        assertEquals(3, tick(7, 7));
        assertEquals(4, tick(8, 8));
    }

    @Test
    public void when_negativeClockIncreasingByOne() {
        histo.reset(-10);
        assertEquals(Long.MIN_VALUE, tick(-10, 1));
        assertEquals(Long.MIN_VALUE, tick(-9, 2));
        assertEquals(Long.MIN_VALUE, tick(-8, 3));
        assertEquals(Long.MIN_VALUE, tick(-7, 4));
        assertEquals(1, tick(-6, 5));
        assertEquals(2, tick(-5, 6));
        assertEquals(3, tick(-4, 7));
        assertEquals(4, tick(-3, 8));
    }

    @Test
    public void when_clockIncreasingByOneStopAndResume() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(Long.MIN_VALUE, tick(2, 2));
        assertEquals(Long.MIN_VALUE, tick(3, 3));
        assertEquals(Long.MIN_VALUE, tick(4, 4));
        assertEquals(Long.MIN_VALUE, tick(4, 5));
        assertEquals(Long.MIN_VALUE, tick(4, 6));
        assertEquals(Long.MIN_VALUE, tick(4, 7));
        assertEquals(1, tick(5, 8));
    }

    @Test
    public void when_clockIncreasingByTwo() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(Long.MIN_VALUE, tick(3, 2));
        assertEquals(1, tick(5, 3));
        assertEquals(2, tick(7, 4));
        assertEquals(3,  tick(9, 5));
    }

    @Test
    public void when_clockIncreasingByThree() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(Long.MIN_VALUE, tick(4, 2));
        assertEquals(1, tick(7, 3));
        assertEquals(2, tick(10, 4));
    }

    @Test
    public void when_clockIncreasingByFour() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(1, tick(5, 2));
        assertEquals(2, tick(9, 3));
        assertEquals(3, tick(13, 4));
        assertEquals(4,  tick(17, 5));
    }

    @Test
    public void when_clockIncreasingByFive() {
        assertEquals(Long.MIN_VALUE, tick(1, 1));
        assertEquals(1, tick(6, 2));
        assertEquals(2, tick(11, 3));
        assertEquals(3, tick(16, 4));
    }

    @Test
    public void when_minValuePunc_then_minValue() {
        for (int i=0; i<100; i++) {
            assertEquals(Long.MIN_VALUE, tick(i, Long.MIN_VALUE));
        }
    }

    private long tick(long now, long topSeq) {
        return histo.tick(now, topSeq);
    }
}
