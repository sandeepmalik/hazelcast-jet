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

package com.hazelcast.jet.connector.hadoop;

import com.hazelcast.core.Member;
import com.hazelcast.jet.*;
import com.hazelcast.jet.Processors.NoopP;
import com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.IndexedInputSplit;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.connector.hadoop.SerializableJobConf.asSerializable;
import static com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.assignSplitsToMembers;
import static com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.printAssignments;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.*;
import static java.util.stream.IntStream.range;
import static org.apache.hadoop.mapred.Reporter.NULL;

/**
 * A processor which reads and emits records from HDFS.
 *
 * The input according to the given {@code InputFormat} is split
 * among the processor instances and each processor instance is responsible
 * for reading a part of the input. The records by default are emitted as
 * {@code Map.Entry<K,V>}, but this can also be transformed to another type
 * using an optional {@code mapper}.
 *
 * Jet cluster should be run on the same nodes as the HDFS nodes for best
 * read performance. If the hosts are aligned, each processor instance will
 * try to read as much local data as possible. A heuristic algorithm is used
 * to assign replicated blocks across the cluster to ensure a
 * well-balanced work distribution between processor instances.
 *
 * @param <K> key type of the records
 * @param <V> value type of the records
 * @param <R> the type of the emitted value
 */
public final class ReadHdfsP<K, V, R> extends AbstractProcessor {

    private final Traverser<R> trav;
    private final DistributedBiFunction<K, V, R> mapper;

    private ReadHdfsP(@Nonnull List<RecordReader> recordReaders, @Nonnull DistributedBiFunction<K, V, R> mapper) {
        this.trav = traverseIterable(recordReaders).flatMap(this::traverseRecordReader);
        this.mapper = mapper;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(trav);
    }

    private Traverser<R> traverseRecordReader(RecordReader<K, V> r) {
        return () -> {
            K key = r.createKey();
            V value = r.createValue();
            try {
                if (r.next(key, value)) {
                    return mapper.apply(key, value);
                }
                r.close();
                return null;
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Returns a meta-supplier of processors that reads from HDFS files.
     * The processors emit entries of type {@code Map.Entry<K,V>}.
     *
     * @param <K>     key type of the records
     * @param <V>     value type of the records
     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @return {@link ProcessorMetaSupplier} supplier
     */
    @Nonnull
    public static <K, V> MetaSupplier<K, V, Entry<K, V>> readHdfs(@Nonnull JobConf jobConf) {
        return readHdfs(jobConf, Util::entry);
    }

    /**
     * Returns a meta-supplier of processors that read HDFS files.
     * It will emit entries of type {@code Map.Entry<K,V>}.
     *
     * @param <K>     key type of the records
     * @param <V>     value type of the records
     * @param <R>     the type of the mapped value
     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @param mapper  mapper which can be used to map the key and value to another value
     * @return {@link ProcessorMetaSupplier} supplier
     */
    @Nonnull
    public static <K, V, R> MetaSupplier<K, V, R> readHdfs(
            @Nonnull JobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper
    ) {
        return new MetaSupplier<>(asSerializable(jobConf), mapper);
    }

    private static class MetaSupplier<K, V, R> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final DistributedBiFunction<K, V, R> mapper;

        private transient Map<Address, List<IndexedInputSplit>> assigned;
        private transient ILogger logger;


        MetaSupplier(@Nonnull SerializableJobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper) {
            this.jobConf = jobConf;
            this.mapper = mapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(ReadHdfsP.class);
            try {
                int totalParallelism = context.totalParallelism();
                InputFormat inputFormat = jobConf.getInputFormat();
                InputSplit[] splits = inputFormat.getSplits(jobConf, totalParallelism);
                IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.length];
                Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits[i]));

                Address[] addrs = context.jetInstance().getCluster().getMembers()
                        .stream().map(Member::getAddress).toArray(Address[]::new);
                assigned = assignSplitsToMembers(indexedInputSplits, addrs, logger);
                printAssignments(assigned, logger);
            } catch (IOException e) {
                throw rethrow(e);
            }
        }


        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(
                    jobConf,
                    assigned.get(address) != null ? assigned.get(address) : emptyList(),
                    mapper);
        }
    }

    private static class Supplier<K, V, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private SerializableJobConf jobConf;
        private List<IndexedInputSplit> assignedSplits;
        private DistributedBiFunction<K, V, R> mapper;

        Supplier(SerializableJobConf jobConf,
                 Collection<IndexedInputSplit> assignedSplits,
                 @Nonnull DistributedBiFunction<K, V, R> mapper
        ) {
            this.jobConf = jobConf;
            this.assignedSplits = assignedSplits.stream().collect(toList());
            this.mapper = mapper;
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits =
                    range(0, assignedSplits.size()).mapToObj(i -> new SimpleImmutableEntry<>(i, assignedSplits.get(i)))
                            .collect(groupingBy(e -> e.getKey() % count,
                                    mapping(Entry::getValue, toList())));
            range(0, count)
                    .forEach(processor -> processorToSplits.computeIfAbsent(processor, x -> emptyList()));
            InputFormat inputFormat = jobConf.getInputFormat();

            return processorToSplits
                    .values().stream()
                    .map(splits -> splits.isEmpty()
                            ? new NoopP()
                            : new ReadHdfsP<>(splits.stream()
                            .map(IndexedInputSplit::getSplit)
                            .map(split -> uncheckCall(() ->
                                    inputFormat.getRecordReader(split, jobConf, NULL)))
                            .collect(toList()), mapper)
                    ).collect(toList());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            jobConf.write(out);
            out.writeObject(assignedSplits);
            out.writeObject(mapper);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            jobConf = new SerializableJobConf();
            jobConf.readFields(in);
            assignedSplits = (List<IndexedInputSplit>) in.readObject();
            mapper = (DistributedBiFunction<K, V, R>) in.readObject();
        }
    }
}
