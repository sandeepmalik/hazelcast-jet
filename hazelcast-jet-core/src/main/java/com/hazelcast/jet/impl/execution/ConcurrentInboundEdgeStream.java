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

import java.util.BitSet;
import java.util.Collection;

import static com.hazelcast.jet.impl.execution.DoneWatermark.DONE_WM;

/**
 * {@code InboundEdgeStream} implemented in terms of a {@code ConcurrentConveyor}. The conveyor has as many
 * 1-to-1 concurrent queues as there are upstream tasklets contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker;
    private Watermark currentWm;
    private final BitSet wmReceived;
    private final BitSet queueDone;
    private final WatermarkDetector wmDetector = new WatermarkDetector();

    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.tracker = new ProgressTracker();
        this.wmReceived = new BitSet(conveyor.queueCount());
        this.queueDone = new BitSet(conveyor.queueCount());
    }

    /**
     * Drains all inbound queues into the {@code dest} collection.
     *
     * <p>Non-watermarks are drained directly. When a {@link Watermark} is encountered in particular queue,
     * we stop draining from that queue and drain other queues, until we receive {@link Object#equals(Object) equal}
     * watermarks from all of them. Only then we insert the watermark to {@code dest} collection.
     *
     * <p>Receiving non-equal watermark produces an error. So does receiving a new watermark, when some
     * queue is already done or a when queue becomes done without emitting expected watermark.
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
            Watermark wm = wmDetector.drainWithWatermarkDetection(conveyor, queueIndex, tracker, dest);
            if (wm == null) {
                continue;
            }
            // we've got watermark, handle it
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
                throw new JetException("Received a new watermark after some processor already completed (wm=" + wm + ')');
            }
            return;
        }
        if (wm == DONE_WM) {
            throw new JetException("Processor completed without first emitting a watermark, that was already emitted by "
                    + "another processor (wm=" + currentWm + ')');
        }
        if (!wm.equals(currentWm)) {
            throw new JetException("Watermark emitted by one processor not equal to watermark emitted by "
                    + "another one, wm1=" + currentWm + ", wm2=" + wm
                    + ", all processors must emit equal watermarks in the same order");
        }
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }
}

