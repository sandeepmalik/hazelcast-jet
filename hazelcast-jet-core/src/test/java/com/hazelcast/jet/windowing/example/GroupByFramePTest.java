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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.windowing.example.SlidingWindowPTest.MockInbox;
import com.hazelcast.util.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.windowing.example.FrameProcessors.groupByFrame;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class GroupByFramePTest {

    private Processor gbf;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        gbf = groupByFrame(
                x -> 77L,
                Entry<Long, Long>::getKey,
                x -> x,
                DistributedCollector.of(
                        MutableLong::new,
                        (acc, e) -> acc.value += e.getValue(),
                        (a, b) -> MutableLong.valueOf(a.value + b.value),
                        a -> a.value
                )
        ).get(1).iterator().next();
        outbox = new ArrayDequeOutbox(1, new int[] {101});
        gbf.init(outbox, mock(Context.class));
    }

    @Test
    public void smokeTest() {
        // Given
        MockInbox inbox = new MockInbox();
        for (long i = 0; i <= 4; i++) {
            inbox.add(entry(i, 1L));
            inbox.add(entry(i, 1L));
            inbox.add(entry(i, 1L));
        }
        for (long i = 0; i <= 4; i++) {
            inbox.add(new Punctuation(i));
        }

        // When
        gbf.process(0, inbox);

        // Then
        for (int i = 0; i <= 4; i++) {
            assertEquals(frame(i, 3), pollOutbox());
            assertEquals(new Punctuation(i), pollOutbox());
        }
        assertEquals(null, pollOutbox());
    }

    private Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }

    private static Frame<Long, MutableLong> frame(long seq, long value) {
        return new Frame<>(seq, 77L, MutableLong.valueOf(value));
    }
}
