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

/**
 * Receives the observed event seqs and returns the punctuation to emit.
 */
public interface PunctuationKeeper {

    /**
     * Called to report observing an event with the given {@code eventSeq}.
     * Returns the punctuation that should be (or have been) emitted before the
     * event. Can also be called in the absence of received events in order to
     * drive the punctuation forward during a stream lull. In this case the
     * argument's value should be {@code Long.MIN_VALUE}.
     *
     * @param eventSeq event's sequence value
     * @return the punctuation sequence. May be {@code Long.MIN_VALUE} if there is
     *         not enough information to determine any punctuation (e.g., no events
     *         observed)
     */
    long reportEvent(long eventSeq);

    long getCurrentPunctuation();
}
