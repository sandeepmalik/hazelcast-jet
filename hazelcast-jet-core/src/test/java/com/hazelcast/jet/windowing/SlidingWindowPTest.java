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
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.stream.DistributedCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.LongStream;

import static com.hazelcast.jet.windowing.FrameProcessors.slidingWindow;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SlidingWindowPTest {

    private Processor swp;
    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        swp = slidingWindow(1, 4, DistributedCollector.of(
                () -> 0L,
                (acc, val) -> { },
                (acc1, acc2) -> acc1 + acc2,
                acc -> acc)
        ).get(1).iterator().next();
        outbox = new ArrayDequeOutbox(1, new int[] {101});
        swp.init(outbox, mock(Context.class));
        inbox = new ArrayDequeInbox();
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        for (long seq = 0; seq <= 4; seq++) {
            inbox.add(frame(seq, 1));
        }
        for (long seq = 1; seq <= 5; seq++) {
            inbox.add(new Punctuation(seq));
        }

        // When
        while (!inbox.isEmpty()) {
            swp.process(0, inbox);
        }

        System.out.println(outbox);

        // Then
        assertEquals(frame(1, 1), pollOutbox());
        assertEquals(frame(2, 2), pollOutbox());
        assertEquals(frame(3, 3), pollOutbox());
        assertEquals(frame(4, 4), pollOutbox());
        assertEquals(frame(5, 4), pollOutbox());
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        for (long seq = 4; seq >= 0; seq--) {
            inbox.add(frame(seq, 1));
        }
        for (long seq = 1; seq <= 5; seq++) {
            inbox.add(new Punctuation(seq));
        }

        // When
        while (!inbox.isEmpty()) {
            swp.process(0, inbox);
        }

        // Then
        assertEquals(frame(1, 1), pollOutbox());
        assertEquals(frame(2, 2), pollOutbox());
        assertEquals(frame(3, 3), pollOutbox());
        assertEquals(frame(4, 4), pollOutbox());
        assertEquals(frame(5, 4), pollOutbox());
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> frameSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(frameSeqsToAdd);
        for (long seq : frameSeqsToAdd) {
            inbox.add(frame(seq, 1));
        }
        for (long i = 1; i <= 100; i++) {
            inbox.add(new Punctuation(i));
        }

        // When
        while (!inbox.isEmpty()) {
            swp.process(0, inbox);
        }

        for (Object o : outbox.queueWithOrdinal(0)) {
            System.out.println(o);
        }

        // Then
        assertEquals(frame(1, 1), pollOutbox());
        assertEquals(frame(2, 2), pollOutbox());
        assertEquals(frame(3, 3), pollOutbox());
        assertEquals(frame(4, 4), pollOutbox());
        for (long seq = 5; seq <= 100; seq++) {
            assertEquals(frame(seq, 4), pollOutbox());

        }
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveWithGaps_then_emitAscending() {
        // Given
        inbox.add(frame(0, 1));
        inbox.add(frame(10, 1));
        inbox.add(frame(11, 1));
        inbox.add(new Punctuation(50));
        inbox.add(frame(50, 3));
        inbox.add(new Punctuation(51));

        // When
        while (!inbox.isEmpty()) {
            swp.process(0, inbox);
        }

        // Then
        assertEquals(frame(1, 1), pollOutbox());
        assertEquals(frame(2, 1), pollOutbox());
        assertEquals(frame(3, 1), pollOutbox());
        assertEquals(frame(4, 1), pollOutbox());

        assertEquals(frame(11, 1), pollOutbox());
        assertEquals(frame(12, 2), pollOutbox());
        assertEquals(frame(13, 2), pollOutbox());
        assertEquals(frame(14, 2), pollOutbox());
        assertEquals(frame(15, 1), pollOutbox());

        assertEquals(frame(51, 3), pollOutbox());

        assertEquals(null, pollOutbox());
    }

    private Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }

    private static Frame<Long, Long> frame(long seq, long value) {
        return new Frame<>(seq, 77L, value);
    }
}
