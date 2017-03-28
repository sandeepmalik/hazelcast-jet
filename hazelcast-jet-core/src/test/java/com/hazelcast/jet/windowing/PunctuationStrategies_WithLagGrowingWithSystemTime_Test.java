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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PunctuationStrategies_WithLagGrowingWithSystemTime_Test {

    private long[] clock = new long[1];
    private PunctuationStrategy p = PunctuationStrategies.withLagGrowingWithSystemTime(2, 3_000_000, () -> clock[0]);

    @Test
    public void when_outOfOrderEvents_then_monotonicPunct() {
        assertEquals(8, p.getPunct(10));
        assertEquals(8, p.getPunct(9));
        assertEquals(8, p.getPunct(8));
        assertEquals(8, p.getPunct(7)); // late event, but nevermind
        assertEquals(9, p.getPunct(11));
    }

    @Test
    public void when_eventsStop_then_punctIncreases() {
        // Given - starting event
        assertEquals(10, p.getPunct(12));

        // When
        for (int i = 0; i < 3 /*catchUpDelay*/; i++) {
            clock[0] += 1_000_000;
            assertEquals(10, p.getPunct(Long.MIN_VALUE));
        }

        // Then - punct increases
        for (int i = 1; i <= 10; i++) {
            clock[0] += 1_000_000;
            assertEquals("at i=" + i, 10 + i, p.getPunct(Long.MIN_VALUE));
        }
    }

    @Test
    public void when_noEventEver_then_increaseFromLongMinValue() {
        // When
        assertEquals(Long.MIN_VALUE, p.getPunct(Long.MIN_VALUE)); // this is the artificial "first item"
        for (int i = 0; i < 3 /*catchUpDelay*/; i++) {
            clock[0] += 1_000_000;
            assertEquals(Long.MIN_VALUE, p.getPunct(Long.MIN_VALUE));
        }

        // Then - punct increases
        for (int i = 1; i <= 10; i++) {
            clock[0] += 1_000_000;
            assertEquals("at i=" + i, Long.MIN_VALUE + i, p.getPunct(Long.MIN_VALUE));
        }
    }

}
