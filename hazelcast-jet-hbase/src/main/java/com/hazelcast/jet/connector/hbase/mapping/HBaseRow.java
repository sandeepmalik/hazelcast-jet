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

package com.hazelcast.jet.connector.hbase.mapping;

import java.util.List;

/**
 * A canonical representation of an HBase row
 */
public interface HBaseRow {

    /**
     * @return rowKey - the bytes representation of the row key
     */
    byte[] rowKey();

    /**
     * A two dimensional list representing the cells of a row
     * the first byte array of inner list represents column family
     * the second byte array of the inner list represents column qualifier
     * the third byte array of the inner list represents column value
     * the fourth (optional) byte array of the inner list represents column time stamp (long value)
     *
     * @return cells - a two dimensional list representing the cells of a row
     */
    List<List<byte[]>> cells();

    /**
     * @return unwrap - returns the source that was used to create this representation.
     * If the row was created from a Java object, then the unwrap() returns the Java object
     * If the row was created from the {@link org.apache.hadoop.hbase.client.Result Result} object then unwrap()
     * returns the Result object.
     */
    Object unwrap();

}
