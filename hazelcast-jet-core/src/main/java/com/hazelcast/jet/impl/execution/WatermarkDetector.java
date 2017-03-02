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
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.function.Predicate;

import java.util.Collection;

import static com.hazelcast.jet.impl.execution.DoneWatermark.DONE_WM;

/**
 * Utility to drain queues in the conveyor, while watching for {@link Watermark}s.
 */
final class WatermarkDetector {
    private Collection<Object> dest;
    private Watermark wm;

    private Predicate<Object> itemHandler = o -> {
        if (o instanceof Watermark) {
            assert wm == null;
            wm = (Watermark) o;
            return false;
        }
        return dest.add(o);
    };

    /**
     * Drains the queue at {@code queueIndex} into a {@code dest} collection, up to the next
     * {@link Watermark}. Also updates the {@code tracker} with new status.
     *
     * @return Watermark, if found, or null
     */
    Watermark drainWithWatermarkDetection(ConcurrentConveyor<Object> conveyor, int queueIndex, ProgressTracker tracker,
            Collection<Object> dest) {
        this.dest = dest;
        wm = null;

        int drainedCount = conveyor.drain(queueIndex, itemHandler);

        // note: progress is reported even if only DONE_ITEM is drained
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, wm == DONE_WM));

        this.dest = null;
        return wm;
    }
}
