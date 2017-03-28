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
 * A class, that will generate punctuations based on event sequence.
 */
public interface PunctuationStrategy {

    /**
     * Based on provided {@code eventSeq}, returns the punctuation, that should be emitted
     * before this event.
     *
     * @param eventSeq Event sequence value or {@code Long.MIN_VALUE}, if we want next
     *                 punctuation without items.
     * @return Punctuation sequence. Can be {@code Long.MIN_VALUE}, if we don't care now.
     */
    long getPunct(long eventSeq);

}
