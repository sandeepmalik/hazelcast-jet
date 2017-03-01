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

import com.hazelcast.jet.impl.util.DoneItem;
import com.hazelcast.util.function.Predicate;

import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;

/**
 * Decorator around an item handler, which detects an attempt to handle the
 * {@link DoneItem#DONE_ITEM}. It doesn't pass it to the wrapped handler, but
 * raises our {@link #done} flag. It is an error to attempt to handle any elements
 * after the {@code DONE_ITEM}.
 */
final class ItemHandlerWithDoneDetector implements Predicate<Object> {
    boolean done;
    Predicate<Object> wrapped;

    ItemHandlerWithDoneDetector() {
    }

    @Override
    public boolean test(Object o) {
        assert !done : "Attempt to add an item after the DONE_ITEM";
        if (o == DONE_ITEM) {
            done = true;
            return false;
        } else {
            return wrapped.test(o);
        }
    }

}
