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
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.util.function.Predicate;

public class ConveyorEmitter implements InboundEmitter {

    private final ItemHandlerWithDoneDetector doneDetector = new ItemHandlerWithDoneDetector();
    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;

    public ConveyorEmitter(ConcurrentConveyor<Object> conveyor, int queueIndex) {
        this.conveyor = conveyor;
        this.queueIndex = queueIndex;
    }

    @Override
    public ProgressState drain(Predicate<Object> itemHandler) {
        doneDetector.wrapped = itemHandler;
        try {
            int drainedCount = conveyor.drain(queueIndex, itemHandler);
            return ProgressState.valueOf(drainedCount > 0, doneDetector.done);
        } finally {
            doneDetector.wrapped = null;
        }
    }

}
