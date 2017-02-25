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

import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.BiFunction;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.UnaryOperator;
import com.hazelcast.jet.stream.DistributedCollector;

import java.util.Set;

import static java.util.Collections.emptySet;

public interface SnapshottingCollector<T, A, R> extends DistributedCollector<T, A, R> {

    UnaryOperator<A> snapshotter();

    static <T, A, R> SnapshottingCollector<T, A, R> of(
            Supplier<A> supplier,
            BiConsumer<A, T> accumulator,
            UnaryOperator<A> snapshotter,
            BinaryOperator<A> combiner,
            Function<A, R> finisher
    ) {
        return new SnapshottingCollector<T, A, R>() {
            @Override
            public Supplier<A> supplier() {
                return supplier;
            }

            @Override
            public BiConsumer<A, T> accumulator() {
                return accumulator;
            }

            @Override
            public UnaryOperator<A> snapshotter() {
                return snapshotter;
            }

            @Override
            public BinaryOperator<A> combiner() {
                return combiner;
            }

            @Override
            public Function<A, R> finisher() {
                return finisher;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return emptySet();
            }
        };
    }
}
