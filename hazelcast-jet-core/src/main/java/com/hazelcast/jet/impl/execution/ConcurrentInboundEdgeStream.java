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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;

import java.util.BitSet;
import java.util.Collection;

/**
 * {@code InboundEdgeStream} implemented in terms of a {@code ConcurrentConveyor}. The conveyor has as many
 * 1-to-1 concurrent queues as there are upstream tasklets contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final InboundEmitter[] emitters;
    private final ProgressTracker tracker;
    private Watermark lastWm;
    private final BitSet wmFound;
    private final CollectionWithWatermarkDetector wmDetector = new CollectionWithWatermarkDetector();

    public ConcurrentInboundEdgeStream(InboundEmitter[] emitters, int ordinal, int priority) {
        this.emitters = emitters;
        this.ordinal = ordinal;
        this.priority = priority;
        this.tracker = new ProgressTracker();
        this.wmFound = new BitSet(emitters.length);
    }

    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        tracker.reset();
        wmDetector.wrapped = dest;
        for (int i = 0; i < emitters.length; i++) {
            InboundEmitter emitter = emitters[i];
            if (emitter == null) {
                continue;
            }
            if (alreadyAtWatermark(i)) {
                tracker.notDone();
                continue;
            }
            wmDetector.wm = null;
            ProgressState result = emitter.drain(wmDetector::add);
            tracker.mergeWith(result);
            if (result.isDone()) {
                emitters[i] = null;
            }
            // No watermark was detected
            if (wmDetector.wm == null) {
                continue;
            }
            validateWatermark();
            wmFound.set(i);
            lastWm = wmDetector.wm;
            if (allWatermarksReceived()) {
                dest.add(lastWm);
                lastWm = null;
                wmFound.clear();
                return tracker.toProgressState();
            }
        }
        return tracker.toProgressState();
    }

    private boolean alreadyAtWatermark(int i) {
        return lastWm != null && wmFound.get(i);
    }

    private boolean allWatermarksReceived() {
        return wmFound.nextClearBit(0) == emitters.length;
    }

    private void validateWatermark() {
        if (lastWm != null && !wmDetector.wm.equals(lastWm))
            throw new JetException("Watermark emitted by one processor not equal to watermark emitted by "
                    + "another processor, wm1=" + lastWm + ", wm2=" + wmDetector.wm
                    + ", all processors must emit equal watermarks in the same order");
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

