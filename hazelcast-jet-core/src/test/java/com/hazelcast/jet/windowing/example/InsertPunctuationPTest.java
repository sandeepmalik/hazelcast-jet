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
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class InsertPunctuationPTest {

    private static final long LAG = 3;

    private MyClock clock;
    private InsertPunctuationP<Item> p;
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
        p = new InsertPunctuationP<>(Item::getTime, LAG, 16L, 3, 3, clock::time);

        outbox = new ArrayDequeOutbox(128, new int[]{128});
        Context context = mock(Context.class);

        p.init(outbox, context);
    }

    @Test
    public void test_processCalledNormally() throws Exception {
        String[] expected = {
                "-- at 100",
                "Punctuation{seq=7}",
                "Item{time=10}",
                "Item{time=8}",
                "-- at 101",
                "Item{time=11}",
                "Item{time=9}",
                "-- at 102",
                "Item{time=12}",
                "Item{time=10}",
                "-- at 103",
                "Punctuation{seq=10}",
                "Item{time=13}",
                "Item{time=11}",
                "-- at 104",
                "-- at 105",
                "-- at 106",
                "-- at 107",
                "-- at 108",
                "-- at 109",
                "-- at 110",
                "-- at 111",
                "-- at 112",
                "-- at 113",
                "-- at 114",
                "-- at 115",
                "-- at 116",
                "-- at 117",
                "Punctuation{seq=11}",
                "-- at 118",
                "-- at 119",
                "-- at 120",
                "Punctuation{seq=13}",
                "-- at 121",
                "-- at 122",
                "-- at 123",
                "-- at 124",
                "-- at 125",
                "-- at 126",
                "-- at 127",
                "-- at 128",
                "-- at 129",
                "-- at 130",
                "-- at 131",
                "Punctuation{seq=38}",
                "Item{time=41}",
                "Item{time=39}",
                "-- at 132",
                "Item{time=42}",
                "Item{time=40}",
                "-- at 133",
                "Item{time=43}",
                "Item{time=41}",
                "-- at 134",
                "Punctuation{seq=41}",
                "Item{time=44}",
                "Item{time=42}",
        };

        List<String> actual = new ArrayList<>();
        for (int eventTime = 10; eventTime < 45; eventTime++) {
            actual.add("-- at " + clock.time());
            if (eventTime < 14 || eventTime > 40) {
                Item item = new Item(eventTime);
                Item oldItem = new Item(eventTime - 2);
                p.tryProcess(0, item);
                p.tryProcess(0, oldItem);
            }

            p.process();

            drainOutbox(actual);
            clock.advance(1);
        }

        assertEquals(toString(Arrays.asList(expected)), toString(actual));
    }

    @Test
    public void test_bigPauseInProcessCalls() throws Exception {
        String[] expected = {
                "-- at 100",
                "Punctuation{seq=7}",
                "Item{time=10}",
                "-- process",
                "-- at 101",
                "Item{time=11}",
                "-- process",
                "-- at 102",
                "Item{time=12}",
                "-- process",
                "-- at 103",
                "Punctuation{seq=10}",
                "Item{time=13}",
                "-- process",
                "-- at 104",
                "-- at 105",
                "-- at 106",
                "-- at 107",
                "-- at 108",
                "-- at 109",
                "-- at 110",
                "-- at 111",
                "-- at 112",
                "-- at 113",
                "-- at 114",
                "-- at 115",
                "-- at 116",
                "-- at 117",
                "-- at 118",
                "-- at 119",
                "-- at 120",
                "-- at 121",
                "-- at 122",
                "-- at 123",
                "-- at 124",
                "-- at 125",
                "-- at 126",
                "-- at 127",
                "-- at 128",
                "-- at 129",
                "-- at 130",
                "Punctuation{seq=37}",
                "Item{time=40}",
                "-- process",
                "-- at 131",
                "-- process",
                "-- at 132",
                "-- process",
                "-- at 133",
                "-- process",
                "-- at 134",
                "-- process",
                "-- at 135",
                "-- process",
                "-- at 136",
                "-- process",
                "-- at 137",
                "-- process",
                "-- at 138",
                "-- process",
                "-- at 139",
                "-- process",
                "-- at 140",
                "-- process",
                "-- at 141",
                "-- process",
                "-- at 142",
                "-- process",
                "-- at 143",
                "-- process",
                "-- at 144",
                "-- process",
                "-- at 145",
                "-- process",
                "-- at 146",
                "-- process",
                "Punctuation{seq=40}",
                "-- at 147",
                "-- process",
                "-- at 148",
                "-- process",
                "-- at 149",
                "-- process",
        };

        List<String> actual = new ArrayList<>();
        for (int eventTime = 10; eventTime < 60; eventTime++) {
            actual.add("-- at " + clock.time());
            if (eventTime < 14 || eventTime >= 40) {
                if (eventTime <= 40) {
                    Item item = new Item(eventTime);
                    p.tryProcess(0, item);
                    drainOutbox(actual);
                }
                // note process() is not called in each iteration
                actual.add("-- process");
                p.process();
                drainOutbox(actual);
            }

            outbox.queueWithOrdinal(0).clear();
            clock.advance(1);
        }

        assertEquals(toString(Arrays.asList(expected)), toString(actual));
    }

    @Test
    public void test_smallPauseInProcessCalls() throws Exception {
        String[] expected = {
                "-- at 100",
                "Punctuation{seq=7}",
                "Item{time=10}",
                "-- process",
                "-- at 101",
                "Item{time=11}",
                "-- process",
                "-- at 102",
                "Item{time=12}",
                "-- process",
                "-- at 103",
                "Punctuation{seq=10}",
                "Item{time=13}",
                "-- process",
                "-- at 104",
                "-- at 105",
                "-- at 106",
                "-- at 107",
                "-- at 108",
                "-- at 109",
                "-- at 110",
                "-- at 111",
                "-- at 112",
                "-- at 113",
                "-- at 114",
                "-- at 115",
                "-- at 116",
                "-- at 117",
                "-- at 118",
                "-- process",
                "Punctuation{seq=12}",
                "-- at 119",
                "-- process",
                "-- at 120",
                "-- process",
                "-- at 121",
                "-- process",
                "Punctuation{seq=13}",
                "-- at 122",
                "-- process",
                "-- at 123",
                "-- process",
                "-- at 124",
                "-- process",
                "-- at 125",
                "-- process",
                "-- at 126",
                "-- process",
                "-- at 127",
                "-- process",
                "-- at 128",
                "-- process",
                "-- at 129",
                "-- process",
                "-- at 130",
                "Punctuation{seq=37}",
                "Item{time=40}",
                "-- process",
                "-- at 131",
                "-- process",
                "-- at 132",
                "-- process",
                "-- at 133",
                "-- process",
                "-- at 134",
                "-- process",
                "-- at 135",
                "-- process",
                "-- at 136",
                "-- process",
                "-- at 137",
                "-- process",
                "-- at 138",
                "-- process",
                "-- at 139",
                "-- process",
                "-- at 140",
                "-- process",
                "-- at 141",
                "-- process",
                "-- at 142",
                "-- process",
                "-- at 143",
                "-- process",
                "-- at 144",
                "-- process",
                "-- at 145",
                "-- process",
                "-- at 146",
                "-- process",
                "Punctuation{seq=40}",
                "-- at 147",
                "-- process",
                "-- at 148",
                "-- process",
                "-- at 149",
                "-- process",
        };

        List<String> actual = new ArrayList<>();
        for (int eventTime = 10; eventTime < 60; eventTime++) {
            actual.add("-- at " + clock.time());
            if (eventTime < 14 || eventTime >= 40) {
                if (eventTime <= 40) {
                    Item item = new Item(eventTime);
                    p.tryProcess(0, item);
                }
            }
            drainOutbox(actual);
            // note process() is not called in each iteration
            if (eventTime < 14 || eventTime >= 28) {
                actual.add("-- process");
                p.process();
                drainOutbox(actual);
            }

            outbox.queueWithOrdinal(0).clear();
            clock.advance(1);
        }

        assertEquals(toString(Arrays.asList(expected)), toString(actual));
    }

    private void drainOutbox(List<String> actual) {
        for (Object o; (o = outbox.queueWithOrdinal(0).poll()) != null; )
            actual.add(o.toString());
    }

    private String toString(List<String> actual) {
        return actual.stream().collect(Collectors.joining("\n"));
    }
}
