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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThrottlingPunctuationTest {

    private static final int MIN_STEP = 2;
    private PunctuationKeeper p;
    private long punc;

    @Before
    public void setup() {
        PunctuationKeeper mockKeeper = new PunctuationKeeper() {

            @Override
            public long reportEvent(long eventSeq) {
                return punc;
            }

            @Override
            public long getCurrentPunctuation() {
                return punc;
            }
        };
        p = mockKeeper.throttle(MIN_STEP);
    }

    @Test
    public void when_puncIncreasing_then_throttleByMinStep() {
        assertPunc(2, 2);
        assertPunc(3, 2);
        assertPunc(4, 4);
        assertPunc(6, 6);
        assertPunc(9, 9);
        assertPunc(10, 9);
        assertPunc(11, 11);
    }

    private void assertPunc(long actualPunc, long throttledPunc) {
        punc = actualPunc;
        assertEquals(throttledPunc, p.reportEvent(0));
        assertEquals(throttledPunc, p.getCurrentPunctuation());

    }

}
