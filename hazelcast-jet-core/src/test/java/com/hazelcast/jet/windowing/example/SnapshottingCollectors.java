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

import com.hazelcast.jet.Distributed.Function;

public final class SnapshottingCollectors {

    private SnapshottingCollectors() {
    }

    public static <T, U, A, R> SnapshottingCollector<T, A, R> mapping(
            Function<? super T, ? extends U> mapper, SnapshottingCollector<? super U, A, R> downstream
    ) {
        return SnapshottingCollector.of(
                downstream.supplier(),
                (A a, T t) -> downstream.accumulator().accept(a, mapper.apply(t)),
                downstream.snapshotter(),
                downstream.combiner(),
                downstream.finisher()
        );
    }

    public static SnapshottingCollector<Number, ?, Long> summingLong() {
        return SnapshottingCollector.of(
                () -> new long[1],
                (a, t) -> a[0] += t.longValue(),
                long[]::clone,
                (a, b) -> {
                    a[0] += b[0];
                    return a;
                },
                a -> a[0]);
    }

    private static class MutableLong {
        public long val;

        public MutableLong() {
        }

        // copy constructor
        public MutableLong(MutableLong o) {
            this.val = o.val;
        }

        @Override
        public String toString() {
            return "{" + val + '}';
        }

        @Override
        public boolean equals(Object o) {
            return o != null && o instanceof MutableLong && ((MutableLong) o).val == val;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(val);
        }
    }

    public static <T> SnapshottingCollector<T, MutableLong, Long> counting() {
        return SnapshottingCollector.of(
                MutableLong::new,
                (a, t) -> a.val++,
                MutableLong::new,
                (a, b) -> {
                    a.val += b.val;
                    return a;
                },
                a -> a.val);
    }
}
