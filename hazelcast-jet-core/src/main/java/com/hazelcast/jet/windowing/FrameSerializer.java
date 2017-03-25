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

import com.hazelcast.jet.impl.execution.init.JetSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Hazelcast serializer of {@link Frame}.
 */
public final class FrameSerializer implements StreamSerializer<Frame> {
    @Override
    public void write(ObjectDataOutput out, Frame object) throws IOException {
        out.writeLong(object.getSeq());
        out.writeObject(object.getKey());
        out.writeObject(object.getValue());
    }

    @Override
    public Frame read(ObjectDataInput in) throws IOException {
        long seq = in.readLong();
        Object key = in.readObject();
        Object value = in.readObject();
        return new Frame<>(seq, key, value);
    }

    @Override
    public int getTypeId() {
        return JetSerializerHook.KEYED_FRAME;
    }

    @Override
    public void destroy() {
    }
}
