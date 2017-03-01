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

package com.hazelcast.jet.impl.execution;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ItemHandlerWithDoneDetectorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ItemHandlerWithDoneDetector handler = new ItemHandlerWithDoneDetector();
    private List<Object> list = new ArrayList<>();

    @Before
    public void setUp() {
        handler.wrapped = list::add;
    }

    @Test
    public void when_addNormalItem_then_acceptIt() {
        assertFalse(handler.done);

        assertTrue(handler.test("a"));
        assertFalse(handler.done);
        assertEquals(list.size(), 1);

        assertTrue(handler.test("b"));
        assertFalse(handler.done);
        assertEquals(list.size(), 2);
    }

    @Test
    public void when_addDoneItem_then_refuseItAndBecomeDone() {
        assertFalse(handler.done);

        assertTrue(handler.test("a"));
        assertFalse(handler.done);
        assertEquals(list.size(), 1);

        assertFalse(handler.test(DONE_ITEM));
        assertTrue(handler.done);
        assertEquals(list.size(), 1);
    }

    @Test
    public void when_addAfterDone_then_fail() {
        assertFalse(handler.done);

        assertTrue(handler.test("a"));
        assertFalse(handler.done);
        assertEquals(list.size(), 1);

        assertFalse(handler.test(DONE_ITEM));
        assertTrue(handler.done);
        assertEquals(list.size(), 1);

        exception.expect(AssertionError.class);
        handler.test("c");
    }

    @Test
    public void when_addMultipleDoneItems_then_fail() {
        assertFalse(handler.done);

        assertTrue(handler.test("a"));
        assertFalse(handler.done);
        assertEquals(list.size(), 1);

        assertFalse(handler.test(DONE_ITEM));
        assertTrue(handler.done);
        assertEquals(list.size(), 1);

        exception.expect(AssertionError.class);
        handler.test(DONE_ITEM);
    }

}