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

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class InterleavePunctuationPTest {

    private static final long LAG = 3;

    private MyClock clock;
    private InterleavePunctuationP<Item> p;
    private ArrayDequeOutbox outbox;

    class Item {
        final long time;

        Item(long time) {
            this.time = time;
        }

        public long getTime() {
            return time;
        }

        @Override
        public String toString() {
            return "Item{time=" + time + '}';
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof Item && this.time == ((Item) o).time;
        }

        @Override
        public int hashCode() {
            return (int) (time ^ (time >>> 32));
        }
    }

    private class MyClock {
        long time;

        public MyClock(long time) {
            this.time = time;
        }

        long time() {
            return time;
        }

        void advance(long by) {
            time += by;
        }
    }

    @Before
    public void setUp() {
        clock = new MyClock(100);
        p = new InterleavePunctuationP<>(Item::getTime, LAG, 16, clock::time);

        outbox = new ArrayDequeOutbox(128, new int[]{128});
        Context context = mock(Context.class);

        p.init(outbox, context);
    }

    @Test
    public void test_processCalledNormally() throws Exception {
        List<Object> expected = new ArrayList<>();
        for (int eventTime = 10; eventTime < 45; eventTime++) {
            expected.clear();
//            System.out.println("-- at " + clock.time());
            if (eventTime < 14 || eventTime > 40) {
                Item item = new Item(eventTime);
                Item oldItem = new Item(eventTime - 2);
                p.tryProcess(0, item);
                p.tryProcess(0, oldItem);
                expected.add(item);
                expected.add(new Punctuation(item.getTime() - LAG));
                expected.add(oldItem);
            }
            if (eventTime == 27) {
                expected.add(new Punctuation(11));
            }
            if (eventTime == 28) {
                expected.add(new Punctuation(12));
            }
            if (eventTime == 29) {
                expected.add(new Punctuation(13));
            }

            p.process();

//            for (Object o : outbox.queueWithOrdinal(0))
//                System.out.println(o);

            assertEquals(expected, new ArrayList<>(outbox.queueWithOrdinal(0)));
            outbox.queueWithOrdinal(0).clear();

            clock.advance(1);
        }
    }

    @Test
    public void test_bigPauseInProcessCalls() throws Exception {
        List<Object> expected = new ArrayList<>();
        for (int eventTime = 10; eventTime < 60; eventTime++) {
            expected.clear();
//            System.out.println("-- at " + clock.time());
            if (eventTime < 14 || eventTime >= 40) {
                if (eventTime <= 40) {
                    Item item = new Item(eventTime);
                    p.tryProcess(0, item);
                    expected.add(item);
                    expected.add(new Punctuation(eventTime - LAG));
                }
                // note process() is not called in each iteration
//                System.out.println("-- process");
                p.process();
            }

            if (eventTime == 56) {
                expected.add(new Punctuation(40));
            }

//            for (Object o : outbox.queueWithOrdinal(0))
//                System.out.println(o);

            outbox.queueWithOrdinal(0).clear();
            clock.advance(1);
        }
    }

    @Test
    public void test_smallPauseInProcessCalls() throws Exception {
        List<Object> expected = new ArrayList<>();
        for (int eventTime = 10; eventTime < 60; eventTime++) {
            expected.clear();
//            System.out.println("-- at " + clock.time());
            if (eventTime < 14 || eventTime >= 40) {
                if (eventTime <= 40) {
                    Item item = new Item(eventTime);
                    p.tryProcess(0, item);
                    expected.add(item);
                    expected.add(new Punctuation(eventTime - LAG));
                }
            }
            // note process() is not called in each iteration
            if (eventTime < 14 || eventTime >= 28) {
//                System.out.println("-- process");
                p.process();
                if (eventTime >= 28 && eventTime <= 29)
                    expected.add(new Punctuation(eventTime - 16));
            }

            if (eventTime == 56) {
                expected.add(new Punctuation(40));
            }

            assertEquals(expected, new ArrayList<>(outbox.queueWithOrdinal(0)));
//            for (Object o : outbox.queueWithOrdinal(0))
//                System.out.println(o);

            outbox.queueWithOrdinal(0).clear();
            clock.advance(1);
        }
    }
}
