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

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.execution.init.JetSerializerHook;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.windowing.example.CombineFramesP.combineFrames;
import static com.hazelcast.jet.windowing.example.GroupByFrameP.groupByFrame;
import static com.hazelcast.jet.windowing.example.GroupByFrameP.groupByFrame;
import static com.hazelcast.jet.windowing.example.SnapshottingCollectors.counting;
import static com.hazelcast.jet.windowing.example.SnapshottingCollectors.mapping;
import static com.hazelcast.jet.windowing.example.SnapshottingCollectors.summingLong;
import static java.lang.Runtime.getRuntime;

public class TradeMonitor {

    private static final Map<String, Integer> TICKERS = new HashMap<String, Integer>() {{
        put("GOOG", 10000);
        put("FB", 15000);
        put("ATVI", 15000);
        put("ADBE", 15000);
        put("AKAM", 15000);
        put("ALXN", 15000);
        put("AMZN", 15000);
        put("AAL", 15000);
        put("AMGN", 15000);
        put("ADI", 15000);
        put("AAPL", 15000);
    }};

    private static final ILogger logger = Logger.getLogger(TradeMonitor.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        try {
            JetConfig cfg = new JetConfig();
            cfg.getHazelcastConfig().getSerializationConfig().addSerializerConfig(new SerializerConfig()
                    .setImplementation(new KeyedFrameSerializer()).setTypeClass(KeyedFrame.class));

            final int defaultLocalParallelism = Math.max(1, getRuntime().availableProcessors() / 2);
            cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(defaultLocalParallelism));

            Jet.newJetInstance(cfg);
            JetInstance jet = Jet.newJetInstance(cfg);

            IStreamMap<String, Integer> initial = jet.getMap("initial");
            Stream<String> lines = Files.lines(Paths.get(TradeMonitor.class.getResource("/nasdaqlisted.txt").toURI()));
            lines.skip(1).map(l -> l.split("\\|")[0]).forEach(t -> initial.put(t, 10000));

            DAG dag = new DAG();
            Vertex tickerSource = dag.newVertex("ticker-source", readMap(initial.getName()));
            Vertex generateEvents = dag.newVertex("generate-events", () -> new TradeGeneratorP(0));
            Vertex peek = dag.newVertex("peek", PeekP::new);
            Vertex groupByFrame = dag.newVertex("group-by-frame",
                    groupByFrame(4, Trade::getTicker,
                            t -> System.currentTimeMillis(), ts -> ts / 1_000,
                            counting()
                    )
            );
            Vertex combineFrames = dag.newVertex("combine-frames",
                    combineFrames(counting()));
            Vertex sink = dag.newVertex("sink", Processors.writeMap("sink")).localParallelism(1);

            dag.edge(between(tickerSource, generateEvents).broadcast().distributed())
               .edge(between(generateEvents, groupByFrame).partitioned(Trade::getTicker, HASH_CODE))
               .edge(between(groupByFrame, combineFrames).partitioned((Function<KeyedFrame, Object>) KeyedFrame::getKey)
                                                         .distributed())
               .edge(between(combineFrames, sink));

//            dag
//               .edge(from(generateEvents, 1).to(peek))
//               .edge(from(groupByFrame, 1).to(peek, 0))
//               .edge(from(combineFrames).to(peek, 0));

            jet.newJob(dag).execute();

            while (true) {
                Long sum = jet.getMap("sink").aggregate(Aggregators.longSum());
                logger.info("Sum: " + sum);
                Thread.sleep(1000);
            }

        } finally {
            Jet.shutdownAll();
        }
    }

    public static class PeekP extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            getLogger().info(item.toString());
            return true;
        }
    }

    public static final class KeyedFrameSerializer implements StreamSerializer<KeyedFrame> {

        @Override
        public void write(ObjectDataOutput out, KeyedFrame object) throws IOException {
            out.writeLong(object.getSeq());
            out.writeObject(object.getKey());
            out.writeObject(object.getValue());
        }

        @Override
        public KeyedFrame read(ObjectDataInput in) throws IOException {
            long seq = in.readLong();
            Object key = in.readObject();
            Object value = in.readObject();
            return new KeyedFrame(seq, key, value);
        }

        @Override
        public int getTypeId() {
            return JetSerializerHook.KEYED_FRAME;
        }

        @Override public void destroy() {

        }
    }
}
