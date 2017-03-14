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

package com.hazelcast.jet;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Miscellaneous utility methods useful in DAG building logic.
 */
public final class Util {
    private Util() {

    }

    /**
     * Returns a {@code Map.Entry} with the given key and value.
     */
    public static <K, V> Entry<K, V> entry(K k, V v) {
        return new SimpleImmutableEntry<>(k, v);
    }

    public static <T1, T2, T3> Triple<T1, T2, T3> triple(T1 t1, T2 t2, T3 t3) {
        return new Triple<>(t1, t2, t3);
    }

    public static final class Triple<T1, T2, T3> {
        public final T1 t1;
        public final T2 t2;
        public final T3 t3;

        Triple(T1 t1, T2 t2, T3 t3) {
            this.t1 = t1;
            this.t2 = t2;
            this.t3 = t3;
        }

        @Override
        public boolean equals(Object o) {
            Triple that;
            return this == o
                    || o instanceof Triple
                        && Objects.equals(this.t1, (that = (Triple) o).t1)
                        && Objects.equals(this.t2, that.t2)
                        && Objects.equals(this.t3, that.t3);
        }

        @Override
        public int hashCode() {
            int hc = 17;
            hc = 37 * hc + t1.hashCode();
            hc = 37 * hc + t2.hashCode();
            hc = 37 * hc + t3.hashCode();
            return hc;
        }
    }
}
