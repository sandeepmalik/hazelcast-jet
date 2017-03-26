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
 * Holds the aggregated result of a session window.
 *
 * @param <K> type of key
 * @param <R> type of aggregated result
 */
public class Session<K, R> {
    private final K key;
    private final R result;
    private final long start;
    private final long end;

    Session(K key, R result, long start, long end) {
        this.key = key;
        this.result = result;
        this.start = start;
        this.end = end;
    }

    /**
     * Returns the session's key.
     */
    public K getKey() {
        return key;
    }

    /**
     * Returns the aggregated result for the session.
     */
    public R getResult() {
        return result;
    }

    /**
     * Returns the starting timestamp of the session.
     */
    public long getStart() {
        return start;
    }

    /**
     * Returns the ending timestamp of the session.
     */
    public long getEnd() {
        return end;
    }
}
