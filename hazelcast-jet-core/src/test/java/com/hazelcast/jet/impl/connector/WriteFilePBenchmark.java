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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.Traversers.traverseStream;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.stream.Collectors.joining;

public class WriteFilePBenchmark {

    private static final boolean FLUSH_EARLY = false;
    private static final int LINE_LENGTH = 100;
    private static final int NUM_LINES = 10_000_000;
    private static final String LINE = Stream.generate(() -> "a").limit(LINE_LENGTH).collect(joining());

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance instance = Jet.newJetInstance();
        Job job = createJob(instance);
        try {
            for (int i = 0; i < 20; i++) {
                long start = System.nanoTime();
                job.execute().get();
                System.out.format("Took %,d ms%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
        } finally {
            instance.shutdown();
        }
    }

    private static Job createJob(JetInstance instance) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new AbstractProcessor() {
            Traverser traverser = traverseStream(Stream.generate(() -> LINE).limit(NUM_LINES));
            @Override
            public boolean complete() {
                return emitFromTraverser(traverser);
            }
        });
        Vertex sink = dag.newVertex("sink", writeFile("bench.txt", ISO_8859_1, true, FLUSH_EARLY));
        dag.edge(between(source.localParallelism(1), sink.localParallelism(1)));
        return instance.newJob(dag);
    }

}
