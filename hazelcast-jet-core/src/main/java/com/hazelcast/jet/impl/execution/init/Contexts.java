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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier.Context;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

public final class Contexts {

    private Contexts() {
    }

    public static class ProcCtx implements Processor.Context {

        private final JetInstance instance;
        private final ILogger logger;
        private final String vertexName;
        private final int index;
        private CompletableFuture<Void> jobFuture;
        private LongSupplier nanoClock;

        public ProcCtx(JetInstance instance, ILogger logger, String vertexName, int index) {
            this.instance = instance;
            this.logger = logger;
            this.vertexName = vertexName;
            this.index = index;
        }

        @Override @Nonnull
        public JetInstance jetInstance() {
            return instance;
        }

        @Override @Nonnull
        public ILogger logger() {
            return logger;
        }

        @Override
        public int index() {
            return index;
        }

        @Override @Nonnull
        public String vertexName() {
            return vertexName;
        }

        /**
         * {@inheritDoc}
         * This implementation may theoretically return {@code null} even though
         * it's annotated as {@code @Nonnull}; however the client code will
         * never observe this object in such a state.
         */
        @Override @Nonnull
        public CompletableFuture<Void> jobFuture() {
            return jobFuture;
        }

        /**
         * {@inheritDoc}
         * This implementation may theoretically return {@code null} even though
         * it's annotated as {@code @Nonnull}; however the client code will
         * never observe this object in such a state.
         */
        @Override @Nonnull
        public LongSupplier nanoClock() {
            return nanoClock;
        }

        public void initFromTasklet(@Nonnull CompletableFuture<Void> jobFuture, @Nonnull LongSupplier nanoClock) {
            assert this.jobFuture == null : "jobFuture already initialized";
            assert this.nanoClock == null : "nanoClock already initialized";
            this.jobFuture = jobFuture;
            this.nanoClock = nanoClock;
        }
    }

    static class ProcSupplierCtx implements ProcessorSupplier.Context {
        private final JetInstance instance;
        private final int perNodeParallelism;

        ProcSupplierCtx(JetInstance instance, int perNodeParallelism) {
            this.instance = instance;
            this.perNodeParallelism = perNodeParallelism;
        }

        @Override @Nonnull
        public JetInstance jetInstance() {
            return instance;
        }

        @Override
        public int localParallelism() {
            return perNodeParallelism;
        }
    }

    static class MetaSupplierCtx implements Context {
        private final JetInstance jetInstance;
        private final int totalParallelism;
        private final int localParallelism;

        MetaSupplierCtx(JetInstance jetInstance, int totalParallelism, int localParallelism) {
            this.jetInstance = jetInstance;
            this.totalParallelism = totalParallelism;
            this.localParallelism = localParallelism;
        }

        @Override @Nonnull
        public JetInstance jetInstance() {
            return jetInstance;
        }

        @Override
        public int totalParallelism() {
            return totalParallelism;
        }

        @Override
        public int localParallelism() {
            return localParallelism;
        }
    }
}

