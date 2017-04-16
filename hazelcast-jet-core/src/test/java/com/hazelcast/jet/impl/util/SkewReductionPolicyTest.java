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

import com.hazelcast.jet.impl.util.SkewReductionPolicy.SkewExceededAction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SkewReductionPolicyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SkewReductionPolicy srd = new SkewReductionPolicy(4, 1000, 500, SkewExceededAction.NO_ACTION);

    @Test
    public void test_queuesKeptInOrder() {
        srd.observePunc(1, 2);
        assertQueuesOrdered();
        srd.observePunc(2, 0);
        assertQueuesOrdered();
        srd.observePunc(3, 3);
        assertQueuesOrdered();
        srd.observePunc(0, 4);
        assertQueuesOrdered();
        // the most ahead becomes even more ahead
        srd.observePunc(0, 5);
        assertQueuesOrdered();

        // the most behind advances, but still the most behind
        srd.observePunc(2, 1);
        assertQueuesOrdered();

        // all queues become equally ahead
        for (int i = 0; i<srd.drainOrderToQIdx.length; i++) {
            srd.observePunc(i, 6);
            assertQueuesOrdered();
        }
    }

    private void assertQueuesOrdered() {
        long lastValue = Long.MIN_VALUE;
        for (int i = 1; i<srd.queuePuncSeqs.length; i++) {
            long thisValue = srd.queuePuncSeqs[srd.drainOrderToQIdx[i]];
            assertTrue("Queues not ordered\nobservedPuncSeqs="
                            + Arrays.toString(srd.queuePuncSeqs) + "\norderedQueues="
                    + Arrays.toString(srd.drainOrderToQIdx),
                    lastValue <= thisValue);
            lastValue = thisValue;
        }

        // assert, that each queue index is unique in orderedQueues
        Set<Integer> set = new HashSet<>();
        for (int i : srd.drainOrderToQIdx)
            set.add(i);

        assertEquals(srd.drainOrderToQIdx.length, set.size());
    }
}
