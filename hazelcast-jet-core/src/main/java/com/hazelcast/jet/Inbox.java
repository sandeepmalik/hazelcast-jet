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

package com.hazelcast.jet;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Queue-like API with special treatment for the watermark item. The inbox
 * is in a special state when it contains just a watermark item. In that
 * state the standard {@link #peek()}, {@link #poll()}, and {@link #remove()}
 * behave as if the inbox is empty. The methods {@link #drain(Consumer) drain()}
 * and {@link #drainTo(Collection) drainTo()} behave as if implemented in terms
 * of {@code poll()} and therefore won't drain the final watermark.
 * <p>
 * <strong>NOTE</strong> that any watermark that is not the last item in
 * the inbox will be given no special treatment.
 * <p>
 * <strong>NOTE</strong> that the inbox may report {@code size() > 0} yet fail
 * to return anything from the dequeuing methods.
 */
public interface Inbox {

    /**
     * Returns the number of items in this inbox.
     */
    int size();

    /**
     * Returns {@code true} if this inbox contains no elements, {@code false}
     * otherwise.
     */
    boolean isEmpty();

    /**
     * Retrieves, but does not remove, the head of this inbox, or returns
     * {@code null} if it is empty or contains just a watermark item.
     */
    Object peek();

    /**
     * Retrieves and removes the head of this inbox, or returns {@code null}
     * if it is empty or contains just a watermark item.
     */
    Object poll();

    /**
     * Retrieves and removes the head of this inbox. This method differs from
     * {@link #poll poll} only in that it throws an exception if the inbox is
     * empty or contains just a watermark item.
     *
     * @throws NoSuchElementException if this inbox is empty
     */
    default Object remove() {
        final Object item = poll();
        if (item == null) {
            throw new NoSuchElementException("remove()");
        }
        return item;
    }

    /**
     * Drains all the items except for any final {@link Watermark} into the
     * provided {@link Collection}.
     *
     * @return the number of items drained
     */
    default <E> int drainTo(Collection<E> target) {
        int drained = 0;
        //noinspection unchecked
        for (E o; (o = (E) poll()) != null; drained++) {
            target.add(o);
        }
        return drained;
    }

    /**
     * Passes each of this inbox's items, except for any final {@link Watermark},
     * to the supplied consumer.
     *
     * @return the number of items drained
     */
    default <E> int drain(Consumer<E> consumer) {
        int consumed = 0;
        //noinspection unchecked
        for (E o; (o = (E) poll()) != null; consumed++) {
            consumer.accept(o);
        }
        return consumed;
    }
}
