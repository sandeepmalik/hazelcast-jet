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

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SlidingWindowPTest {

    SlidingWindowP<Long> swp;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        swp = new SlidingWindowP<>(4, SnapshottingCollector.of(null, null, null, (a, b) -> a + b, x -> x));
        outbox = new ArrayDequeOutbox(1, new int[]{1});
        swp.init(outbox, mock(Context.class));
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        MockInbox inbox = new MockInbox();
        for (long i = 0; i <= 4; i++) {
            inbox.add(entry(i, 1L));
        }

        // When
        swp.process(0, inbox);

        // Then
        assertEquals(entry(3L, 4L), outbox.queueWithOrdinal(0).poll());
        assertEquals(entry(4L, 4L), outbox.queueWithOrdinal(0).poll());
        assertEquals(null, outbox.queueWithOrdinal(0).poll());
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        MockInbox inbox = new MockInbox();
        for (long i = 4; i >= 0; i--) {
            inbox.add(entry(i, 1L));
        }

        // When
        swp.process(0, inbox);

        // Then
        assertEquals(entry(3L, 4L), outbox.queueWithOrdinal(0).poll());
        assertEquals(entry(4L, 4L), outbox.queueWithOrdinal(0).poll());
        assertEquals(null, outbox.queueWithOrdinal(0).poll());
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> streamSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(streamSeqsToAdd);
        MockInbox inbox = new MockInbox();
        for (Long i : streamSeqsToAdd) {
            inbox.add(entry(i, 1L));
        }

        // When
        swp.process(0, inbox);

        // Then
        for (long i = 3; i < 100; i++) {
            assertEquals(entry(i, 4L), outbox.queueWithOrdinal(0).poll());

        }
        assertEquals(null, outbox.queueWithOrdinal(0).poll());
    }

    static class MockInbox extends ArrayDeque implements Inbox {}
}
