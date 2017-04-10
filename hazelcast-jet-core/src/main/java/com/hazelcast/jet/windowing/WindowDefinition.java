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

package com.hazelcast.jet.windowing;

import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Contains parameters that define the window that should be
 * computed from the infinite data stream.
 */
public class WindowDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long frameLength;
    private final long frameOffset;
    private final long windowLength;

    public WindowDefinition(long frameLength, long frameOffset, long framesPerWindow) {
        checkPositive(frameLength, "frameLength must be positive");
        checkNotNegative(frameOffset, "frameOffset must not be negative");
        checkPositive(framesPerWindow, "framesPerWindow must be positive");
        this.frameLength = frameLength;
        this.frameOffset = frameOffset;
        this.windowLength = frameLength * framesPerWindow;
    }

    public long frameLength() {
        return frameLength;
    }

    public long frameOffset() {
        return frameOffset;
    }

    public long windowLength() {
        return windowLength;
    }

    /**
     * Returns the sequence for the highest frame ending at or below the given event sequence.
     */
    long floorFrameSeq(long seq) {
        return seq -  Math.floorMod(seq - frameOffset, frameLength);
    }

    /**
     * Returns the sequence for the first frame ending above the given sequence
     */
    long higherFrameSeq(long seq) {
        return floorFrameSeq(seq) + frameLength;
    }
}
