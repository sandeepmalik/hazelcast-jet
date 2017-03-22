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

package com.hazelcast.jet.windowing.example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Javadoc pending.
 */
public final class Frame<K, V> implements Serializable, Map.Entry<K,V> {
    private final long seq;
    private final K key;
    private final V value;

    Frame(long seq, K key, V value) {
        this.seq = seq;
        this.key = key;
        this.value = value;
    }

    public long getSeq() {
        return seq;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + Long.hashCode(seq);
        hc = 73 * hc + key.hashCode();
        hc = 73 * hc + value.hashCode();
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        final Frame that;
        return this == obj
                || obj instanceof Frame
                && this.seq == (that = (Frame) obj).seq
                && this.key.equals(that.key)
                && this.value.equals(that.value);
    }

    @Override
    public String toString() {
        //hack
        String valueStr = value instanceof long[] ? Arrays.toString((long[])value) : value.toString();
        return "Frame{seq=" + seq + ", key=" + key + ", value=" + valueStr + '}';
    }
}
