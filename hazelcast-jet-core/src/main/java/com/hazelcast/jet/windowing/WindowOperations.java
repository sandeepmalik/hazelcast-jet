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

import com.hazelcast.jet.Distributed;

public final class WindowOperations {

    private WindowOperations() {

    }

    public static <T> WindowOperation<T, ?, Long> counting() {
        return reducing(0L, e -> 1L, java.lang.Long::sum, (a, b) -> a - b);
    }

    public static <T, U> WindowOperation<T, ?, U> reducing(U identity,
                                                           Distributed.Function<? super T, ? extends U> mapper,
                                                           Distributed.BinaryOperator<U> combineOp,
                                                           Distributed.BinaryOperator<U> deductOp) {
        return new WindowOperationImpl<>(
                boxSupplier(identity),
                (a, t) -> a[0] = combineOp.apply(a[0], mapper.apply(t)),
                (a, b) -> {
                    a[0] = combineOp.apply(a[0], b[0]);
                    return a;
                },
                (a, b) -> {
                    a[0] = deductOp.apply(a[0], b[0]);
                    return a;
                },
                a -> a[0]);
    }

    static <T> Distributed.Supplier<T[]> boxSupplier(T identity) {
        return () -> (T[]) new Object[]{identity};
    }
}
