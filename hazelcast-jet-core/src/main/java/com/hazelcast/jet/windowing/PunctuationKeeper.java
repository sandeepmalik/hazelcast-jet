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

import com.hazelcast.jet.stream.DistributedCollector;

/**
 * A strategy object that "keeps the event time" for a single data
 * (sub)stream. The event seq of every observed item should be reported to
 * this object and it will respond with the current value of the
 * punctuation. Punctuation may also advance even in the absence of
 * observed events; {@link #getCurrentPunctuation()} can be called at any
 * time to see this change.
 */
public interface PunctuationKeeper {

    /**
     * Called to report the observation of an event with the given {@code
     * eventSeq}. Returns the punctuation that should be (or have been) emitted
     * before the event.
     *
     * @param eventSeq event's sequence value
     * @return the punctuation sequence. May be {@code Long.MIN_VALUE} if there is
     *         insufficient information to determine any punctuation (e.g., no events
     *         observed)
     */
    long reportEvent(long eventSeq);

    /**
     * Called to get the current punctuation in the absence of an observed event.
     * The punctuation may advance just based on the passage of time.
     */
    long getCurrentPunctuation();

    /**
     * Returns a new punctuation keeper that throttles this keeper's output
     * by suppressing advancement by less than the supplied {@code minStep}.
     * Punctuation returned from the wrapped keeper that is less than {@code
     * minStep} ahead of the top punctuation returned from this keeper is
     * ignored.
     */
    default PunctuationKeeper throttleByMinStep(long minStep) {
        return new PunctuationKeeper() {

            private long nextPunc = Long.MIN_VALUE;
            private long currPunc = Long.MIN_VALUE;

            @Override
            public long reportEvent(long eventSeq) {
                long newPunc = PunctuationKeeper.this.reportEvent(eventSeq);
                return advanceThrottled(newPunc);
            }

            @Override
            public long getCurrentPunctuation() {
                long newPunc = PunctuationKeeper.this.getCurrentPunctuation();
                return advanceThrottled(newPunc);
            }

            private long advanceThrottled(long newPunc) {
                if (newPunc < nextPunc) {
                    return currPunc;
                }
                nextPunc = newPunc + minStep;
                currPunc = newPunc;
                return newPunc;
            }
        };
    }

    /**
     * Returns a new punctuation keeper that throttles this keeper's output by
     * suppressing advancement within the same frame, as defined by the {@code
     * winDef} parameter. The window definition should match the one supplied to
     * the downstream {@link WindowingProcessors#groupByFrame(
     * com.hazelcast.jet.Distributed.Function,
     * com.hazelcast.jet.Distributed.ToLongFunction, WindowDefinition,
     * DistributedCollector) groupByFrame} processor.
     */
    default PunctuationKeeper throttleByFrame(WindowDefinition winDef) {
        return new PunctuationKeeper() {
            private long lastFrameSeq = Long.MIN_VALUE;
            private long lastPunc = Long.MIN_VALUE;

            @Override
            public long reportEvent(long eventSeq) {
                long newPunc = PunctuationKeeper.this.reportEvent(eventSeq);
                return advanceThrottled(newPunc);
            }

            @Override
            public long getCurrentPunctuation() {
                long newPunc = PunctuationKeeper.this.getCurrentPunctuation();
                return advanceThrottled(newPunc);
            }

            private long advanceThrottled(long newPunc) {
                long frameSeq = winDef.floorFrameSeq(newPunc);
                if (frameSeq <= lastFrameSeq) {
                    return lastPunc;
                }
                lastFrameSeq = frameSeq;
                lastPunc = newPunc;
                return newPunc;
            }
        };
    }
}
