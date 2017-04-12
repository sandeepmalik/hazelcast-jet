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

import com.hazelcast.util.Preconditions;

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

    public WindowDefinition(long frameLength, long framesPerWindow) {
        this(frameLength, 0, framesPerWindow);
    }

    public WindowDefinition(long frameLength, long frameOffset, long framesPerWindow) {
        checkPositive(frameLength, "frameLength must be positive");
        checkNotNegative(frameOffset, "frameOffset must not be negative");
        checkPositive(framesPerWindow, "framesPerWindow must be positive");
        this.frameLength = frameLength;
        this.frameOffset = frameOffset;
        this.windowLength = frameLength * framesPerWindow;
    }

    /**
     * Returns the length of each frame
     */
    public long frameLength() {
        return frameLength;
    }

    /**
     * Returns the offset for each frame
     */
    public long frameOffset() {
        return frameOffset;
    }

    /**
     * Returns the total length of a window
     */
    public long windowLength() {
        return windowLength;
    }

    /**
     * Returns a new window definition where all the frames are shifted by the given offset.
     *
     * Given a sequence of events {@code 0, 1, 2, 3, ...}, and a tumbling window of @{code windowLength = 4},
     * with no offset the windows will have the sequences  {@code [0..4), [4..8), ... }. With {@code offset = 2}
     * they would have the sequences {@code [2..6), [6..12), ... }
     */
    public WindowDefinition withOffset(long offset) {
        return new WindowDefinition(frameLength, offset, windowLength / frameLength );
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

    /**
     * Returns a new definition for a sliding window of length {@code windowLength}
     * and sliding by {@code slideBy}. Sliding windows may have overlapping elements between
     * the windows. The total length of the window must be a multiple of the amount being slided by.
     *
     * Given a sequence of events {@code 0, 1, 2, 3, ...}, {@code windowLength = 4} and {@code slideBy = 2},
     * the following resulting windows will be generated {@code [0..4), [2..6), [4..8), [6..10), ... }
     *
     * @param windowLength the total length of each window. Must be a multiple {@code slideBy}.
     * @param slideBy the amount to slide the window by.
     */
    public static WindowDefinition slidingWindow(long windowLength, long slideBy) {
        Preconditions.checkTrue(windowLength % slideBy == 0, "windowLength must be a multiple of slideBy");
        return new WindowDefinition(slideBy,windowLength/slideBy);
    }

    /**
     * Returns a new tumbling window of length {@code windowLength}. Tumbling windows do not
     * have any overlapping elements between windows.
     *
     * Given a sequence of events {@code 0, 1, 2, 3, ...} and {@code windowLength = 4}, the
     * following resulting windows will be generated {@code [0..4), [4..8), [8..12), ... }
     */
    public static WindowDefinition tumblingWindow(long windowLength) {
        return slidingWindow(windowLength, windowLength);
    }


}
