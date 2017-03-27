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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.util.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.util.Map.Entry;

import static com.hazelcast.jet.Projections.entryKey;
import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SessionWindowPTest {
    private SessionWindowP swp;
    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(1, new int[] {100});
        swp = new SessionWindowP<Entry<String, Long>, String, MutableLong, Long>(
                10,
                Entry::getValue,
                entryKey(),
                DistributedCollector.of(
                        MutableLong::new,
                        (acc, e) -> acc.value++,
                        (a, b) -> MutableLong.valueOf(a.value + b.value),
                        a -> a.value
                ));
        swp.init(outbox, mock(Processor.Context.class));
    }

    @Test
    public void smokeTest() {
        inbox.add(entry("a", 1L));
        inbox.add(entry("a", 12L));
        inbox.add(entry("a", 6L));
        inbox.add(entry("a", 30L));
        inbox.add(entry("a", 35L));
        inbox.add(entry("a", 40L));
        inbox.add(new Punctuation(100));
        swp.process(0, inbox);
        assertEquals(new Session<>("a", 3L, 1, 22), pollOutbox());
        assertEquals(new Session<>("a", 3L, 30, 50), pollOutbox());

        assertTrue("keyToIvToAcc not empty", swp.keyToIvToAcc.isEmpty());
        assertTrue("deadlineToKeys not empty", swp.deadlineToKeys.isEmpty());
    }

    private Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }
}
