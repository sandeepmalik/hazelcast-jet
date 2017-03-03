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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.BufferObjectDataOutput;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Javadoc pending.
 */
public class ReceiverTaskletTest {

    private ReceiverTasklet t;
    private InternalSerializationService serService;

    @Before
    public void before() {
        t = new ReceiverTasklet(new MockOutboundCollector(2), 3, 100);
        serService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void test() {
        final BufferObjectDataOutput out = serService.createObjectDataOutput();
    }
}
