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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.stream.DistributedCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.LongStream;

import static com.hazelcast.jet.windowing.example.FrameProcessors.slidingWindow;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SlidingWindowPTest {

    private Processor swp;
    private MockInbox inbox;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        swp = slidingWindow(4, DistributedCollector.of(() -> null, (a, b) -> {}, (Long a, Long b) -> a + b, x -> x))
                .get(1).iterator().next();
        outbox = new ArrayDequeOutbox(1, new int[] {101});
        swp.init(outbox, mock(Context.class));
        inbox = new MockInbox();
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        for (long i = 0; i <= 4; i++) {
            inbox.add(frame(i, 1));
        }
        for (long i = 0; i <= 4; i++) {
            inbox.add(new Punctuation(i));
        }

        // When
        swp.process(0, inbox);

        // Then
        assertEquals(frame(0, 1), pollOutbox());
        assertEquals(frame(1, 2), pollOutbox());
        assertEquals(frame(2, 3), pollOutbox());
        assertEquals(frame(3, 4), pollOutbox());
        assertEquals(frame(4, 4), pollOutbox());
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        for (long i = 4; i >= 0; i--) {
            inbox.add(frame(i, 1));
        }
        for (long i = 0; i <= 4; i++) {
            inbox.add(new Punctuation(i));
        }

        // When
        swp.process(0, inbox);

        // Then
        assertEquals(frame(0, 1), pollOutbox());
        assertEquals(frame(1, 2), pollOutbox());
        assertEquals(frame(2, 3), pollOutbox());
        assertEquals(frame(3, 4), pollOutbox());
        assertEquals(frame(4, 4), pollOutbox());
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> streamSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(streamSeqsToAdd);
        for (long i : streamSeqsToAdd) {
            inbox.add(frame(i, 1));
        }
        for (long i = 0; i < 100; i++) {
            inbox.add(new Punctuation(i));
        }

        // When
        swp.process(0, inbox);

        // Then
        assertEquals(frame(0, 1), pollOutbox());
        assertEquals(frame(1, 2), pollOutbox());
        assertEquals(frame(2, 3), pollOutbox());
        assertEquals(frame(3, 4), pollOutbox());
        for (long i = 4; i < 100; i++) {
            assertEquals(frame(i, 4), pollOutbox());

        }
        assertEquals(null, pollOutbox());
    }

    private Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }

    private static Frame<Long, Long> frame(long seq, long value) {
        return new Frame<>(seq, 77L, value);
    }

    static class MockInbox extends ArrayDeque implements Inbox {}
}
