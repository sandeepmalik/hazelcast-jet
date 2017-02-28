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

import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

/**
 * Does the computation needed to transform zero or more input data streams into
 * zero or more output streams. Each input/output stream corresponds to one edge
 * on the vertex represented by this processor. The correspondence between a
 * stream and an edge is established via the edge's <em>ordinal</em>.
 * <p>
 * The special case of zero input streams applies to a <em>source</em> vertex,
 * which gets its data from the environment. The special case of zero output
 * streams applies to a <em>sink</em> vertex, which pushes its data to the
 * environment.
 * <p>
 * The processor accepts input from instances of {@link Inbox} and pushes its
 * output to an instance of {@link Outbox}.
 * <p>
 * The processing methods should limit the amount of data they output per
 * invocation because the outbox will not be emptied until the processor yields
 * control back to its caller. Specifically, {@code Outbox} has a method {@link
 * Outbox#hasReachedLimit hasReachedLimit()} that can be tested to see whether
 * it's time to stop pushing more data into it.  There is also a finer-grained
 * method {@link Outbox#hasReachedLimit(int) hasReachedLimit(ordinal)}, which
 * tells the state of an individual output bucket.
 * <p>
 * If this processor declares itself as "cooperative" ({@link #isCooperative()}
 * returns {@code true}, the default), it should also limit the amount of time
 * it spends per call because it will participate in a cooperative multithreading
 * scheme.
 */
public interface Processor {

    /**
     * Initializes this processor with the outbox that the processing methods
     * must use to deposit their output items. This method will be called exactly
     * once and strictly before any calls to processing methods
     * ({@link #process(int, Inbox)}, {@link #tryProcessWatermark(int, Watermark)},
     * {@link #complete()}).
     * <p>
     * The default implementation does nothing.
     */
    default void init(@Nonnull Outbox outbox, @Nonnull Context context) {
    }

    /**
     * Processes some items in the supplied inbox. Removes the items it's
     * done with. Does not remove an item until it is done with it.
     * <p>
     * The default implementation does nothing.
     *
     * @param ordinal ordinal of the edge the item comes from
     * @param inbox   the inbox containing the pending items
     */
    default void process(int ordinal, @Nonnull Inbox inbox) {
    }

    /**
     * Tries to process the supplied watermark item, which was received over
     * the supplied ordinal. May choose to process only partially and return
     * {@code false}, in which case it will be called again later with the
     * same {@code (ordinal, item)} combination.
     * <p>
     * The default implementation throws an {@code UnsupportedOperationException}.
     * <p>
     * <strong>NOTE:</strong> unless the processor doesn't differentiate between
     * its inbound edges, the first choice should be leaving this method alone
     * and instead overriding the specific {@code tryProcessWatermarkN()}
     * methods for each ordinal the processor expects.
     *
     * @param ordinal ordinal of the edge that delivered the item
     * @param wm      watermark item to be processed
     * @return {@code true} if this item has now been processed,
     *         {@code false} otherwise.
     */
    default boolean tryProcessWatermark(int ordinal, Watermark wm) {
        return true;
    }

    /**
     * Called after all the inputs are exhausted. If it returns {@code false}, it will be
     * invoked again until it returns {@code true}. After this method is called, no other
     * processing methods will be called on this processor.
     *
     * @return {@code true} if the completing step is now done, {@code false} otherwise.
     */
    default boolean complete() {
        return true;
    }

    /**
     * Tells whether this processor is able to participate in cooperative multithreading.
     * This means that each invocation of a processing method will take a reasonably small
     * amount of time (up to a millisecond). A cooperative processor should not attempt
     * any blocking I/O operations.
     * <p>
     * If this processor declares itself non-cooperative, it will be allocated a dedicated
     * Java thread. Otherwise it will be allocated a tasklet which shares a thread with other
     * tasklets.
     */
    default boolean isCooperative() {
        return true;
    }


    /**
     * Context passed to the processor in the {@link #init(Outbox, Processor.Context) init()} call.
     */
    interface Context {

        /**
         * Returns the current Jet instance
         */
        @Nonnull
        JetInstance jetInstance();

        /**
         *  Return a logger for the processor
         */
        @Nonnull
        ILogger logger();

        /**
         * Returns the index of the current processor among all the processors created for this vertex on this node.
         */
        int index();

        /***
         * Returns the name of the vertex associated with this processor
         */
        @Nonnull
        String vertexName();


    }
}
