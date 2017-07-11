/*
 *
 *  * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.hazelcast.jet.connector.hbase;

import com.hazelcast.jet.*;
import com.hazelcast.jet.connector.hbase.mapping.EntityMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseObjectMapper;
import com.hazelcast.jet.connector.hbase.mapping.HBaseRow;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Edge.between;
import static org.apache.hadoop.hbase.HBaseConfiguration.create;
import static org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE;
import static org.apache.hadoop.hbase.mapreduce.TableInputFormatBase.TABLE_ROW_TEXTKEY;

@Category({QuickTest.class, ParallelTest.class})
public class ReadHBasePTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
    }

    @Test
    public void testReadHBaseP() throws IOException, ExecutionException, InterruptedException {
        JetInstance instance1 = Jet.newJetClient();

        final String zkQuorum = "dal-appblx084-16.prod.walmart.com,dal-appblx084-14.prod.walmart.com,dal-appblx110-24.prod.walmart.com," +
                "dal-155-18-conblx-001-07.prod.walmart.com,dal-155-18-conblx-001-08.prod.walmart.com";
        final int zkPort = 2181;

        Scan s = new Scan();
        //s.setRowPrefixFilter(Bytes.toBytes(3));
        s.setStartRow(Bytes.toBytes(1191));
        s.setStopRow(Bytes.toBytes(1192));
        s.setCacheBlocks(false);

        final Configuration conf = create();
        conf.set(INPUT_TABLE, "SELLER_DATA");
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
        //conf.set(MAPREDUCE_INPUT_AUTOBALANCE, String.valueOf(true));
        conf.set(TABLE_ROW_TEXTKEY, String.valueOf(false));

        final DAG dag = new DAG();
        final Vertex source = dag.newVertex("source", ReadHBaseP.readHBase(s, conf, new SrcMapper()));
        final Vertex sink = dag.newVertex("sink", WriteHBaseP.writeHBaseP(conf, new SinkMapper()));

        dag.edge(between(source, sink));

        final long now = System.currentTimeMillis();
        instance1.newJob(dag).execute().get();
        System.out.println("time: " + (System.currentTimeMillis() - now));
        System.out.println("Job Done");
        Jet.shutdownAll();
    }

    private static class SrcMapper extends EntityMapper<HBaseRow, SellerData> {
        public SrcMapper() {
            super(SellerData.class);
        }

        @Override
        public SellerData apply(HBaseRow row) {
            return HBaseObjectMapper.map(row, SellerData.class);
        }
    }

    private static class SinkMapper extends EntityMapper<SellerData, HBaseRow> {
        public SinkMapper() {
            super(SellerData.class);
        }

        @Override
        public HBaseRow apply(SellerData sellerData) {
            return HBaseObjectMapper.map(sellerData);
        }
    }

}
