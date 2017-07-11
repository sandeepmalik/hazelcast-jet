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

package com.hazelcast.jet.connector.hbase;

import com.hazelcast.jet.*;
import com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.IndexedInputSplit;
import com.hazelcast.jet.connector.hbase.mapping.EntityMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseObjectMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseRow;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.security.Credentials;

import javax.annotation.Nonnull;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.assignSplitsToMembers;
import static com.hazelcast.jet.connector.hadoop.utils.SplitsCalculator.printAssignments;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.lang.Math.max;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hbase.client.ConnectionFactory.createConnection;
import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString;
import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertStringToScan;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

/**
 * A supplier which reads and emits records from an HBase {@link Scan}.
 * The scan is split according to the region server boundaries.
 * <p>
 * Jet cluster should be run on the same nodes as the {@link org.apache.hadoop.hbase.regionserver.HRegionServer}
 * nodes for best read performance. If the hosts are aligned, each supplier instance will
 * try to read as much local data as possible. A heuristic algorithm is used
 * to assign splits across the cluster to ensure a
 * well-balanced work distribution between supplier instances. Once a split is assigned
 * to a particular member, the split is further divided into micro splits if <code>localParallelism</code>
 * is more than the total number of splits assigned to the member.
 * <p>
 * The emitted records are of type {@link HBaseRow}. Optionally, if an entityClass is provided,
 * then the processor will map the {@link HBaseRow} object
 * to an instance of the given entity class using the {@link HBaseObjectMapper} mapping framework.
 * <p>
 * The supplier also honors the MAPREDUCE_INPUT_AUTOBALANCE property of
 * {@link org.apache.hadoop.hbase.mapreduce.TableInputFormatBase}
 * however, since an auto rebalance can potentially merge smaller regions, it may be sub-optimal
 * to set this property if Jet nodes are co-located with {@link org.apache.hadoop.hbase.regionserver.HRegionServer}
 * <p>
 * If Jet nodes are not co-local then setting this property should result in more efficient scans.
 */
public class ReadHBaseP<T> extends AbstractProcessor {

    private final Iterator<TableSplit> splits;
    private Supplier<T> supplier;
    private Traverser<?> traverser = () -> null;
    private TableSplit current;
    private ResultScanner resultScanner;
    private Table table;
    private String processorName;

    private ReadHBaseP(List<TableSplit> splits, Supplier<T> supplier) {
        this.splits = splits.iterator();
        this.supplier = supplier;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        current = splits.hasNext() ? splits.next() : null;
        if (current != null)
            table = supplier.connection.getTable(current.getTable());
        openScanner();
        processorName = context.vertexName() + "[" + context.globalProcessorIndex() + "]";
    }

    private void openScanner() throws IOException {
        if (current != null) {
            final Scan scan = new Scan(supplier.scan);
            scan.setStartRow(current.getStartRow());
            scan.setStopRow(current.getEndRow());
            resultScanner = table.getScanner(scan);
            getLogger().info(processorName + ": initiating scan for range: " +
                    toStringBinary(scan.getStartRow()) + "-" + toStringBinary(scan.getStopRow()));
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        try {
            if (emitFromTraverser(traverser)) {
                Result[] results = resultScanner.next(1000);
                if (results.length > 0) {
                    traverser = traverseIterable(stream(results).map(HBaseObjectMapper::map).map(r -> supplier.mapper.apply(r)).collect(toList()));
                } else {
                    if (splits.hasNext()) {
                        current = splits.next();
                        openScanner();
                        return false;
                    } else return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public static ProcessorMetaSupplier readHBase(Scan scan, Configuration conf) {
        return readHBase(scan, conf, identity());
    }

    public static <T> ProcessorMetaSupplier readHBase(Scan scan, Configuration conf, Class<T> entityClass) {
        return readHBase(scan, conf,
                new EntityMapper<HBaseRow, T>(entityClass) {
                    @Override
                    public T apply(HBaseRow row) {
                        return HBaseObjectMapper.map(row, entityClass);
                    }
                });
    }

    public static <T> ProcessorMetaSupplier readHBase(Scan scan, Configuration conf, DistributedFunction<HBaseRow, T> mapper) {
        return new MetaSupplier<>(scan, conf, mapper);
    }

    private static class MetaSupplier<T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private transient ILogger logger;
        private final String encScan;
        private final Properties overlay;
        private final DistributedFunction<HBaseRow, T> mapper;

        private MetaSupplier(Scan scan, Configuration conf, DistributedFunction<HBaseRow, T> mapper) {
            try {
                this.encScan = convertScanToString(scan);
                this.overlay = fromConf(conf);
                this.mapper = mapper;
            } catch (IOException e) {
                throw rethrow(e);
            }
        }

        @Override
        public void init(@Nonnull Context context) {
            logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(ReadHBaseP.class);
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            try {
                final Scan scan = convertStringToScan(encScan);
                final Configuration conf = toConf(overlay);
                logger.info("initiating the table splits");
                final TableInputFormat tableInputFormat = new TableInputFormat();
                tableInputFormat.setConf(conf);
                tableInputFormat.setScan(scan);
                List<InputSplit> splits = tableInputFormat.getSplits(new JobContext() {
                    @Override
                    public Configuration getConfiguration() {
                        return conf;
                    }

                    @Override
                    public Credentials getCredentials() {
                        return null;
                    }

                    @Override
                    public JobID getJobID() {
                        return null;
                    }

                    @Override
                    public int getNumReduceTasks() {
                        return 0;
                    }

                    @Override
                    public Path getWorkingDirectory() throws IOException {
                        return null;
                    }

                    @Override
                    public Class<?> getOutputKeyClass() {
                        return null;
                    }

                    @Override
                    public Class<?> getOutputValueClass() {
                        return null;
                    }

                    @Override
                    public Class<?> getMapOutputKeyClass() {
                        return null;
                    }

                    @Override
                    public Class<?> getMapOutputValueClass() {
                        return null;
                    }

                    @Override
                    public String getJobName() {
                        return null;
                    }

                    @Override
                    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                        return null;
                    }

                    @Override
                    public RawComparator<?> getSortComparator() {
                        return null;
                    }

                    @Override
                    public String getJar() {
                        return null;
                    }

                    @Override
                    public RawComparator<?> getCombinerKeyGroupingComparator() {
                        return null;
                    }

                    @Override
                    public RawComparator<?> getGroupingComparator() {
                        return null;
                    }

                    @Override
                    public boolean getJobSetupCleanupNeeded() {
                        return false;
                    }

                    @Override
                    public boolean getTaskCleanupNeeded() {
                        return false;
                    }

                    @Override
                    public boolean getProfileEnabled() {
                        return false;
                    }

                    @Override
                    public String getProfileParams() {
                        return null;
                    }

                    @Override
                    public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
                        return null;
                    }

                    @Override
                    public String getUser() {
                        return null;
                    }

                    @Override
                    public boolean getSymlink() {
                        return false;
                    }

                    @Override
                    public Path[] getArchiveClassPaths() {
                        return new Path[0];
                    }

                    @Override
                    public URI[] getCacheArchives() throws IOException {
                        return new URI[0];
                    }

                    @Override
                    public URI[] getCacheFiles() throws IOException {
                        return new URI[0];
                    }

                    @Override
                    public Path[] getLocalCacheArchives() throws IOException {
                        return new Path[0];
                    }

                    @Override
                    public Path[] getLocalCacheFiles() throws IOException {
                        return new Path[0];
                    }

                    @Override
                    public Path[] getFileClassPaths() {
                        return new Path[0];
                    }

                    @Override
                    public String[] getArchiveTimestamps() {
                        return new String[0];
                    }

                    @Override
                    public String[] getFileTimestamps() {
                        return new String[0];
                    }

                    @Override
                    public int getMaxMapAttempts() {
                        return 0;
                    }

                    @Override
                    public int getMaxReduceAttempts() {
                        return 0;
                    }
                });

                if (splits.size() < addresses.size()) {
                    // we have sub optimal splits, let's break the splits further to achieve maximum parallelization
                    splits = splitSplits(splits, addresses.size());
                    if (splits.size() < addresses.size())
                        throw new IllegalStateException("could not break splits to cover all members, this is highly unusual");
                }

                IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.size()];

                // We plan to use the SplitsCalculator from ReadHdfsP to avoid duplicating the code of
                // member assignment. However, the ReadHdfsP uses TableSplit from mapred package and we plan to use
                // the more current mapreduce package, which among other things, calculates auto re-balancing of
                // splits based on data skew. That should be more efficient if the Jet nodes are not co-local
                final List<InputSplit> dup = splits;
                final Map<Integer, Long> splitSizes = new HashMap<>();
                Arrays.setAll(indexedInputSplits, i -> {
                    final TableSplit ts = (TableSplit) dup.get(i);
                    splitSizes.put(i, ts.getLength());
                    return new IndexedInputSplit(i, new org.apache.hadoop.hbase.mapred.TableSplit(
                            ts.getTable(), ts.getStartRow(), ts.getEndRow(), ts.getRegionLocation()));
                });

                final Map<Address, List<IndexedInputSplit>> assigned = assignSplitsToMembers(indexedInputSplits,
                        addresses.toArray(new Address[addresses.size()]), logger);
                printAssignments(assigned, logger);

                    /*final List<List<InputSplit>> partitions = partition(splits, splits.size() / addresses.size());
                    if (partitions.size() > addresses.size()) {
                        // distribute the last sub list to others:
                        int[] index = {0};
                        partitions.get(partitions.size() - 1).forEach(s -> partitions.get(index[0]++).add(s));
                    }
                    final Map<Address, Supplier> shards = new HashMap<>();
                    for (int i = 0; i < addresses.size(); i++) {
                        shards.put(addresses.get(i), new Supplier(newArrayList(partitions.get(i)), scan, overlay, entityClass));
                    }
                    final StringBuilder sb = new StringBuilder();
                    shards.forEach((k, v) -> sb.append(k).append(" => ").append(v.splits).append("\n"));
                    logger.info("splits distribution: " + sb.toString());
                    return shards::get;*/

                return assigned.entrySet().stream().collect(toMap(Entry::getKey, e -> {
                    final List<InputSplit> splits2 = e.getValue().stream().
                            map(iis -> {
                                final org.apache.hadoop.hbase.mapred.TableSplit ts =
                                        (org.apache.hadoop.hbase.mapred.TableSplit) iis.getSplit();
                                return new TableSplit(ts.getTable(), ts.getStartRow(), ts.getEndRow(),
                                        ts.getRegionLocation(), splitSizes.get(iis.getIndex()));
                            }).collect(toList());
                    return new Supplier<>(splits2, encScan, overlay, mapper);
                }))::get;
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        private List<InputSplit> splitSplits(List<InputSplit> splits, int desiredSize) {
            if (splits.isEmpty() || splits.size() >= desiredSize)
                return splits;
            long max = Long.MIN_VALUE;
            int index = -1;
            for (int i = 0; i < splits.size(); i++) {
                TableSplit t = (TableSplit) splits.get(i);
                if (t.getLength() > max) {
                    max = t.getLength();
                    index = i;
                }
            }
            final TableSplit largest = (TableSplit) splits.remove(index);
            final byte[][] subSplits = Bytes.split(largest.getStartRow(), largest.getEndRow(), 1);
            splits.add(new TableSplit(largest.getTable(), subSplits[0], subSplits[1], largest.getRegionLocation(), largest.getLength() / 2));
            splits.add(new TableSplit(largest.getTable(), subSplits[1], subSplits[2], largest.getRegionLocation(), largest.getLength() / 2));
            return splitSplits(splits, desiredSize);
        }
    }

    static Properties fromConf(Configuration conf) {
        try {
            final Method getOverlay = conf.getClass().getDeclaredMethod("getOverlay");
            getOverlay.setAccessible(true);
            return (Properties) getOverlay.invoke(conf);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static TableSplit toTableSplit(byte[] bytes) {
        try {
            final TableSplit ts = new TableSplit();
            ts.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
            return ts;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static byte[] fromTableSplit(TableSplit tableSplit) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            tableSplit.write(new DataOutputStream(baos));
            return baos.toByteArray();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    static Configuration toConf(Properties props) {
        final Configuration conf = HBaseConfiguration.create();
        props.forEach((k, v) -> conf.set((String) k, (String) v));
        return conf;
    }

    private static final class Supplier<T> implements ProcessorSupplier {

        private List<byte[]> encSplits;
        private String encScan;
        private DistributedFunction<HBaseRow, T> mapper;
        private Properties overlay;
        private transient Scan scan;
        private transient Connection connection;
        private transient List<TableSplit> splits;
        private transient ProcessorSupplier.Context context;
        private transient ILogger logger;

        Supplier(List<InputSplit> splits, String encScan, Properties overlay, DistributedFunction<HBaseRow, T> mapper) {
            this.encSplits = splits.stream().map(t -> (TableSplit) t).map(ReadHBaseP::fromTableSplit).collect(toList());
            this.encScan = encScan;
            this.mapper = mapper;
            this.overlay = overlay;
        }

        @Override
        public void init(@Nonnull Context context) {
            try {
                this.context = context;
                logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(ReadHBaseP.class);
                final Configuration configuration = toConf(overlay);
                connection = createConnection(configuration);
                scan = convertStringToScan(encScan);
                splits = encSplits.stream().map(ReadHBaseP::toTableSplit).collect(toList());
            } catch (IOException e) {
                throw rethrow(e);
            }

        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            if (splits.size() > context.localParallelism()) {
                logger.info("local parallelism is under sized, some scans will run sequentially. Try increasing the local parallelism");
                final List<List<TableSplit>> lists = IntStream.range(0, count).boxed().map(i -> new ArrayList<TableSplit>()).collect(toList());
                final int[] index = {0};
                splits.forEach(s -> lists.get(index[0]++ % lists.size()).add(s));
                return lists.stream().map(l -> new ReadHBaseP<>(l, this)).collect(toList());
            } else {
                logger.info("local parallelism is over sized, creating micro splits");
                final long totalSize = splits.stream().mapToLong(TableSplit::getLength).sum();
                final List<ReadHBaseP> processors = new ArrayList<>();
                final int[] pCounter = {0};
                final int[] sCounter = {0};
                splits.forEach(s -> {
                    int p = -1;
                    if (sCounter[0] == splits.size() - 1) {
                        p = max(1, context.localParallelism() - pCounter[0]);
                    } else {
                        p = max(1, (int) (((s.getLength() + 0.0) / totalSize) * context.localParallelism()));
                    }
                    pCounter[0] += p;
                    sCounter[0]++;
                    if (p > 1) {
                        logger.info("splitting split " + s + " into " + p + " micro splits");
                        final byte[][] subs = Bytes.split(s.getStartRow(), s.getEndRow(), p - 1);
                        for (int i = 1; i < subs.length; i++) {
                            final TableSplit sts = new TableSplit(s.getTable(), subs[i - 1], subs[i], s.getLocations()[0]);
                            processors.add(new ReadHBaseP<>(singletonList(sts), this));
                        }
                    } else {
                        processors.add(new ReadHBaseP<>(singletonList(s), this));
                    }
                });
                return processors;
            }
        }

        @Override
        public void complete(Throwable error) {
            if (connection != null)
                try {
                    connection.close();
                } catch (IOException e) {
                    throw rethrow(e);
                }
        }
    }

}
