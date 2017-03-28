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

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.windowing.FrameProcessors.GroupByFrameP;
import com.hazelcast.util.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.windowing.FrameProcessors.groupByFrame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class GroupByFramePTest {

    private GroupByFrameP<Entry, Long, MutableLong> gbf;
    private ArrayDequeOutbox outbox;

    @Before
    public void before() {
        gbf = (GroupByFrameP<Entry, Long, MutableLong>) groupByFrame(
                x -> 77L,
                Entry<Long, Long>::getKey,
                4, 0,
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
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        inbox.add(entry(0L, 1L)); // to frame 4
        inbox.add(entry(1L, 1L)); // to frame 4
        inbox.add(new Punctuation(3)); // does not close anything
        inbox.add(entry(2L, 1L)); // to frame 4
        inbox.add(new Punctuation(4)); // closes frame 4
        inbox.add(entry(2L, 1L)); // dropped
        inbox.add(entry(4L, 1L)); // to frame 8
        inbox.add(entry(5L, 1L)); // to frame 8
        inbox.add(entry(8L, 1L)); // to frame 12
        inbox.add(new Punctuation(6)); // will not close anything
        inbox.add(new Punctuation(7)); // will not close anything
        inbox.add(entry(4L, 1L)); // to frame 8, accepted, despite of punctuation(4), as the frame is not closed yet
        inbox.add(entry(8L, 1L)); // to frame 12
        inbox.add(new Punctuation(8)); // will close frame 8
        inbox.add(entry(8L, 1L)); // to frame 12
        inbox.add(entry(7L, 1L)); // dropped
        inbox.add(new Punctuation(21)); // will close everything

        // When
        gbf.process(0, inbox);

        // Then
        assertEquals(new Punctuation(0), pollOutbox());
        assertEquals(frame(4, 3L), pollOutbox());
        assertEquals(new Punctuation(4), pollOutbox());
        assertEquals(frame(8, 3L), pollOutbox());
        assertEquals(new Punctuation(8), pollOutbox());
        assertEquals(frame(12, 3L), pollOutbox());
        assertEquals(new Punctuation(12), pollOutbox());
        assertEquals(new Punctuation(20), pollOutbox());
        assertEquals(null, pollOutbox());

        assertTrue("map not empty after emitting everyting", gbf.seqToKeyToFrame.isEmpty());
    }

    @Test
    public void when_noEvents_then_punctsOnOutput() {
        // Given
        ArrayDequeInbox inbox = new ArrayDequeInbox();
        inbox.add(new Punctuation(2)); // closes frame 0
        inbox.add(new Punctuation(3)); // will not close anything
        inbox.add(new Punctuation(4)); // closes frame 4
        inbox.add(new Punctuation(5)); // will not close anything
        inbox.add(new Punctuation(6)); // will not close anything
        inbox.add(new Punctuation(8)); // closes frame 8
        inbox.add(new Punctuation(20)); // closes frames up to 20

        // When
        gbf.process(0, inbox);

        for (Object o : outbox.queueWithOrdinal(0)) {
            System.out.println(o);
        }

        // Then
        assertEquals(new Punctuation(0), pollOutbox());
        assertEquals(new Punctuation(4), pollOutbox());
        assertEquals(new Punctuation(8), pollOutbox());
        assertEquals(new Punctuation(20), pollOutbox());
        assertEquals(null, pollOutbox());

        assertTrue("map not empty after emitting everyting", gbf.seqToKeyToFrame.isEmpty());
    }

    private Object pollOutbox() {
        return outbox.queueWithOrdinal(0).poll();
    }

    private static Frame<Long, MutableLong> frame(long seq, long value) {
        return new Frame<>(seq, 77L, MutableLong.valueOf(value));
    }
}
