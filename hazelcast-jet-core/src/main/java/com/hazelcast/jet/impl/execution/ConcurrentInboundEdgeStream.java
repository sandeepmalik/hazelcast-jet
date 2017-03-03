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

import com.hazelcast.internal.util.concurrent.update.ConcurrentConveyor;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.function.Predicate;

import java.util.BitSet;
import java.util.Collection;

import static com.hazelcast.jet.impl.execution.DoneWatermark.DONE_WM;

/**
 * {@code InboundEdgeStream} implemented in terms of a {@code ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker;
    private final BitSet wmReceived;
    private final BitSet queueDone;
    private final WatermarkDetector wmDetector = new WatermarkDetector();
    private Watermark currentWm;

    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.tracker = new ProgressTracker();
        this.wmReceived = new BitSet(conveyor.queueCount());
        this.queueDone = new BitSet(conveyor.queueCount());
    }

    /**
     * Drains all inbound queues into the {@code dest} collection. After
     * encountering a {@link Watermark} in a particular queue, stops draining from
     * it and drains other queues until it receives {@link Object#equals(Object)
     * equal} watermarks from all of them. At that point it adds the watermark to
     * the {@code dest} collection.
     * <p>
     * Receiving a non-equal watermark produces an error. So does receiving a new
     * watermark while some queue is already done or when a queue becomes done without
     * emitting the expected watermark.
     */
    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        tracker.reset();
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            if (queueDone.get(queueIndex)) {
                continue;
            }
            if (alreadyAtWatermark(queueIndex)) {
                tracker.notDone();
                continue;
            }
            Watermark wm = drainWithWatermarkDetection(queueIndex, dest);
            if (wm == null) {
                continue;
            }
            // we've got a watermark, handle it
            validateWatermark(wm);
            if (wm == DONE_WM) {
                queueDone.set(queueIndex);
                continue;
            }
            wmReceived.set(queueIndex);
            currentWm = wm;
            if (allWatermarksReceived()) {
                dest.add(currentWm);
                currentWm = null;
                wmReceived.clear();
                break;
            }
        }
        return tracker.toProgressState();
    }

    private boolean alreadyAtWatermark(int i) {
        return currentWm != null && wmReceived.get(i);
    }

    private boolean allWatermarksReceived() {
        return wmReceived.nextClearBit(0) == conveyor.queueCount();
    }

    private void validateWatermark(Watermark wm) {
        if (currentWm == null) {
            if (wm != null && wm != DONE_WM && queueDone.nextSetBit(0) >= 0) {
                throw new JetException(
                        "Received a new watermark after some processor already completed (wm=" + wm + ')');
            }
            return;
        }
        if (wm == DONE_WM) {
            throw new JetException("Processor completed without first emitting a watermark" +
                    " that was already emitted by another processor (wm=" + currentWm + ')');
        }
        if (!wm.equals(currentWm)) {
            throw new JetException("Watermark emitted by one processor not equal to watermark emitted by "
                    + "another one, wm1=" + currentWm + ", wm2=" + wm
                    + ", all processors must emit equal watermarks in the same order");
        }
    }

    /**
     * Drains the queue at {@code queueIndex} into a {@code dest} collection, up
     * to the next {@link Watermark}. Also updates the {@code tracker} with new
     * status.
     *
     * @return Watermark, if found, or null
     */
    private Watermark drainWithWatermarkDetection(int queueIndex, Collection<Object> dest) {
        wmDetector.dest = dest;
        wmDetector.wm = null;

        int drainedCount = conveyor.drain(queueIndex, wmDetector);
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, wmDetector.wm == DONE_WM));

        wmDetector.dest = null;
        return wmDetector.wm;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Watermark}s.
     * When encountering a watermark, prevents draining more items.
     */
    private static final class WatermarkDetector implements Predicate<Object> {
        Collection<Object> dest;
        Watermark wm;

        @Override
        public boolean test(Object o) {
            if (o instanceof Watermark) {
                assert wm == null : "Received a watermark item, but this.wm != null";
                wm = (Watermark) o;
                return false;
            }
            return dest.add(o);
        }
    }
}
