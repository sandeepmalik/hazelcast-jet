/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.concurrent.update;

import java.util.Collection;

/**
 * A container for items processed in sequence.
 *
 * @param <E> type of items in the pipe.
 */
public interface Pipe<E> {
    /**
     * The number of items added to this container since creation.
     *
     * @return the number of items added.
     */
    long addedCount();

    /**
     * The number of items removed from this container since creation.
     *
     * @return the number of items removed.
     */
    long removedCount();

    /**
     * The maximum capacity of this container to hold items.
     *
     * @return the capacity of the container.
     */
    int capacity();

    /**
     * Get the remaining capacity for items in the container given the
     * current size.
     *
     * @return remaining capacity of the container
     */
    int remainingCapacity();

    /**
     * Drains the items available in the queue to the supplied item handler.
     * The handler returns a boolean which decides whether to continue draining.
     * If it returns {@code false}, this method refrains from draining further
     * items and returns.
     *
     * @param itemHandler the element handler
     * @return the number of drained items
     */
    int drain(ToBooleanFunction<? super E> itemHandler);

    /**
     * Drain available items into the provided {@link Collection} up to a
     * provided maximum limit of items.
     *
     * If possible, implementations should use smart batching to best handle
     * burst traffic.
     *
     * @param target in to which elements are drained.
     * @param limit  of the maximum number of elements to drain.
     * @return the number of elements actually drained.
     */
    int drainTo(Collection<? super E> target, int limit);
}
