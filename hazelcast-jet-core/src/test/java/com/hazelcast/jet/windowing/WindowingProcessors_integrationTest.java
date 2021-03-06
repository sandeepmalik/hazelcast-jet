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

import com.hazelcast.core.IList;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Processors.NoopP;
import com.hazelcast.jet.Vertex;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.windowing.PunctuationPolicies.limitingLagAndLull;
import static com.hazelcast.jet.windowing.StreamingTestSupport.streamToString;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindowSingleStage;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindowStage1;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindowStage2;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class WindowingProcessors_integrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStageProcessor;

    @Parameters(name = "singleStageProcessor={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @After
    public void shutdownAll() {
        shutdownFactory();
    }

    @Test
    public void smokeTest() throws Exception {
        runTest(
                singletonList(new MockEvent("a", 10, 1)),
                asList(
                        new TimestampedEntry<>(1000, "a", 1L),
                        new TimestampedEntry<>(2000, "a", 1L)
                ));
    }

    private void runTest(List<MockEvent> sourceEvents, List<TimestampedEntry<String, Long>> expectedOutput)
            throws Exception {
        JetInstance instance = super.createJetMember();

        WindowDefinition wDef = slidingWindowDef(2000, 1000);
        WindowOperation<Object, ?, Long> counting = counting();

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", streamList(sourceEvents)).localParallelism(1);
        Vertex insertPP = dag.newVertex("insertPP", insertPunctuation(MockEvent::getTimestamp,
                () -> limitingLagAndLull(500, 1000).throttleByFrame(wDef)))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeList("sink"));

        dag.edge(between(source, insertPP).oneToMany());

        if (singleStageProcessor) {
            Vertex sliwp = dag.newVertex("sliwp", slidingWindowSingleStage(
                    MockEvent::getKey, MockEvent::getTimestamp, wDef, counting));
            dag
                    .edge(between(insertPP, sliwp).partitioned(MockEvent::getKey).distributed())
                    .edge(between(sliwp, sink).oneToMany());

        } else {
            Vertex gbfp = dag.newVertex("gbfp", slidingWindowStage1(
                    MockEvent::getKey, MockEvent::getTimestamp, wDef, counting));
            Vertex sliwp = dag.newVertex("sliwp", slidingWindowStage2(wDef, counting));
            dag
                    .edge(between(insertPP, gbfp).partitioned(MockEvent::getKey))
                    .edge(between(gbfp, sliwp).partitioned(entryKey()).distributed())
                    .edge(between(sliwp, sink).oneToMany());
        }

        instance.newJob(dag).execute();

        IList<TimestampedEntry<String, Long>> sinkList = instance.getList("sink");

        assertTrueEventually(() -> assertTrue(sinkList.size() == expectedOutput.size()));
        // wait a little more and make sure, that there are no more frames
        Thread.sleep(2000);

        String expected = streamToString(expectedOutput.stream());
        String actual = streamToString(new ArrayList<>(sinkList).stream());
        assertEquals(expected, actual);
    }

    /**
     * Returns a {@link ProcessorMetaSupplier}, that will emit contents of a list and the NOT complete.
     * Emits from single node and single processor instance.
     */
    private static ProcessorMetaSupplier streamList(List<?> sourceList) {
        return addresses -> address -> count ->
                IntStream.range(0, count)
                        .mapToObj(i -> i == 0 ? new StreamListP(sourceList) : new NoopP())
                        .collect(toList());
    }

    private static class MockEvent implements Serializable {
        private final String key;
        private final long timestamp;
        private final long value;

        private MockEvent(String key, long timestamp, long value) {
            this.key = key;
            this.timestamp = timestamp;
            this.value = value;
        }

        String getKey() {
            return key;
        }

        long getTimestamp() {
            return timestamp;
        }

        long getValue() {
            return value;
        }
    }

    private static class StreamListP extends AbstractProcessor {
        private final List<?> list;

        StreamListP(List<?> list) {
            this.list = list;
        }

        @Override
        public boolean complete() {
            for (Object o : list) {
                emit(o);
            }
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                // fall through to returning true
            }
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }
}
