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
import com.hazelcast.jet.connector.hbase.mapping.EntityMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseObjectMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseRow;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.logging.ILogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.hazelcast.jet.connector.hbase.ReadHBaseP.fromConf;
import static com.hazelcast.jet.connector.hbase.ReadHBaseP.toConf;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.ConnectionFactory.createConnection;
import static org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE;

/**
 * A processor which consumes records and writes them to HBase table.
 * The records must be of type {@link HBaseRow}. Alternatively, a mapper can be
 * specified, which can convert any arbitrary data to {@link HBaseRow}.
 * A third variation handles data using mapping framework defined in {@link HBaseObjectMapper} to convert
 * any Java object to an {@link HBaseRow}
 */
public final class WriteHBaseP<T> extends AbstractProcessor {

    private transient final Supplier<T> supplier;
    private transient Table table;

    public WriteHBaseP(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        table = supplier.connection.getTable(TableName.valueOf(supplier.configuration.get(INPUT_TABLE)));
    }

    @Override
    protected void processAny(int ordinal, @Nonnull Inbox inbox) throws Exception {
        final List<Object> items = new ArrayList<>();
        inbox.drainTo(items);
        final List<Put> puts = items.stream().map(this::toPut).collect(toList());
        System.out.println("doing puts");
        System.out.println(puts);
        //table.put(puts);
    }

    private Put toPut(Object item) {
        @SuppressWarnings("unchecked")
        T t = (T) item;
        final HBaseRow row = supplier.mapper.apply(t);
        final Put p = new Put(row.rowKey());
        row.cells().forEach(cells -> p.addColumn(cells.get(0), cells.get(1), cells.get(2)));
        return p;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        System.out.println("doing puts");
        System.out.println(toPut(item));
        //table.put(toPut(item));
        return true;
    }

    @Override
    public boolean complete() {
        try {
            if (table != null)
                table.close();
        } catch (IOException e) {
            throw rethrow(e);
        }
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Create a {@link ProcessorMetaSupplier} object, which can distribute HBase 'Put' operations.
     * The incoming data must be of type {@link HBaseRow}
     *
     * @param conf - HBase Configuration
     * @return ProcessorMetaSupplier
     */
    public static ProcessorMetaSupplier writeHBaseP(Configuration conf) {
        return writeHBaseP(conf, identity());
    }

    /**
     * Create a {@link ProcessorMetaSupplier} object, which can distribute HBase 'Put' operations
     *
     * @param conf        - HBase Configuration
     * @param entityClass - the class of incoming data type
     * @return ProcessorMetaSupplier
     */
    public static <T> ProcessorMetaSupplier writeHBaseP(Configuration conf, Class<T> entityClass) {
        return writeHBaseP(conf,
                new EntityMapper<T, HBaseRow>(entityClass) {
                    @Override
                    public HBaseRow apply(T t) {
                        return HBaseObjectMapper.map(t);
                    }
                });
    }

    /**
     * Create a {@link ProcessorMetaSupplier} object, which can distribute HBase 'Put' operations
     *
     * @param conf   - HBase Configuration
     * @param mapper - mapper function to convert incoming data to {@link HBaseRow}
     * @return ProcessorMetaSupplier
     */
    public static <T> ProcessorMetaSupplier writeHBaseP(Configuration conf, DistributedFunction<T, HBaseRow> mapper) {
        return ProcessorMetaSupplier.of(new Supplier<>(conf, mapper));
    }

    private static class Supplier<T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private transient Connection connection;
        private transient Configuration configuration;

        private Properties overlay;
        private final DistributedFunction<T, HBaseRow> mapper;

        private Supplier(Configuration configuration, DistributedFunction<T, HBaseRow> mapper) {
            this.overlay = fromConf(configuration);
            this.mapper = mapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            final ILogger logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(WriteHBaseP.class);
            try {
                logger.info("creating HBase connection");
                configuration = toConf(overlay);
                connection = createConnection(configuration);
            } catch (IOException e) {
                throw rethrow(e);
            }
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new WriteHBaseP<>(this))
                    .limit(count)
                    .collect(toList());
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
