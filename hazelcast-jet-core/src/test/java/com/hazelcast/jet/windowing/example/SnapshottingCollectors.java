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
import com.hazelcast.util.MutableLong;

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

    public static <T> SnapshottingCollector<T, MutableLong, Long> counting() {
        return SnapshottingCollector.of(
                MutableLong::new,
                (a, t) -> a.value++,
                a -> MutableLong.valueOf(a.value),
                (a, b) -> {
                    a.value += b.value;
                    return a;
                },
                a -> a.value);
    }
}
