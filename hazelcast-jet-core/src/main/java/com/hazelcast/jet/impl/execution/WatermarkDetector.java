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

final class WatermarkDetector implements Predicate<Object> {
    private Collection<Object> dest;
    private Watermark wm;

    @Override
    public boolean test(Object o) {
        if (o instanceof Watermark) {
            wm = (Watermark) o;
            return false;
        }
        return dest.add(o);
    }

    Watermark drainWithWatermarkDetection(ConcurrentConveyor<Object> conveyor, int queueIndex, ProgressTracker tracker, Collection<Object> dest) {
        this.dest = dest;
        wm = null;

        int drainedCount = conveyor.drain(queueIndex, this);
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, wm == DONE_WM));

        this.dest = null;
        return wm;
    }
}
