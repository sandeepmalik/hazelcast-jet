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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.windowing.TimestampedEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public final class JetSerializerHook {

    /**
     * Start of reserved space for Jet-specific serializers.
     * Any ID greater than this number might be used by some other Hazelcast serializer.
     * For more information, {@see SerializationConstants}
     */
    public static final int JET_RESERVED_SPACE_START = SerializationConstants.JET_SERIALIZER_FIRST;

    public static final int MAP_ENTRY = -300;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -301;
    public static final int OBJECT_ARRAY = -302;
    public static final int TIMESTAMPED_ENTRY = -303;
    public static final int LONG_ACC = -304;
    public static final int DOUBLE_ACC = -305;
    public static final int MUTABLE_REFERENCE = -306;
    public static final int LIN_TREND_ACC = -307;
    public static final int LONG_LONG_ACC = -308;
    public static final int LONG_DOUBLE_ACC = -309;

    // reserved for hadoop module: -380 to -390

    /**
     * End of reserved space for Jet-specific serializers.
     * Any ID less than this number might be used by some other Hazelcast serializer.
     */
    public static final int JET_RESERVED_SPACE_END = SerializationConstants.JET_SERIALIZER_LAST;

    private JetSerializerHook() {
    }

    public static final class ObjectArray implements SerializerHook<Object[]> {

        @Override
        public Class<Object[]> getSerializationType() {
            return Object[].class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Object[]>() {

                @Override
                public int getTypeId() {
                    return OBJECT_ARRAY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Object[] array) throws IOException {
                    out.writeInt(array.length);
                    for (int i = 0; i < array.length; i++) {
                        out.writeObject(array[i]);
                    }
                }

                @Override
                public Object[] read(ObjectDataInput in) throws IOException {
                    int length = in.readInt();
                    Object[] array = new Object[length];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = in.readObject();
                    }
                    return array;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class MapEntry implements SerializerHook<Map.Entry> {

        @Override
        public Class<Entry> getSerializationType() {
            return Map.Entry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Map.Entry>() {
                @Override
                public int getTypeId() {
                    return MAP_ENTRY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Entry object) throws IOException {
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public Entry read(ObjectDataInput in) throws IOException {
                    return entry(in.readObject(), in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class TimestampedEntrySerializer implements SerializerHook<TimestampedEntry> {

        @Override
        public Class<TimestampedEntry> getSerializationType() {
            return TimestampedEntry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<TimestampedEntry>() {
                @Override
                public void write(ObjectDataOutput out, TimestampedEntry object) throws IOException {
                    out.writeLong(object.getTimestamp());
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public TimestampedEntry read(ObjectDataInput in) throws IOException {
                    long timestamp = in.readLong();
                    Object key = in.readObject();
                    Object value = in.readObject();
                    return new TimestampedEntry<>(timestamp, key, value);
                }

                @Override
                public int getTypeId() {
                    return JetSerializerHook.TIMESTAMPED_ENTRY;
                }

                @Override
                public void destroy() {
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class LongAccSerializer implements SerializerHook<LongAccumulator> {

        @Override
        public Class<LongAccumulator> getSerializationType() {
            return LongAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongAccumulator>() {
                @Override
                public int getTypeId() {
                    return LONG_ACC;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, LongAccumulator object) throws IOException {
                    out.writeLong(object.get());
                }

                @Override
                public LongAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongAccumulator(in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class DoubleAccSerializer implements SerializerHook<DoubleAccumulator> {

        @Override
        public Class<DoubleAccumulator> getSerializationType() {
            return DoubleAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<DoubleAccumulator>() {
                @Override
                public int getTypeId() {
                    return DOUBLE_ACC;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, DoubleAccumulator object) throws IOException {
                    out.writeDouble(object.get());
                }

                @Override
                public DoubleAccumulator read(ObjectDataInput in) throws IOException {
                    return new DoubleAccumulator(in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class MutableReferenceSerializer implements SerializerHook<MutableReference> {

        @Override
        public Class<MutableReference> getSerializationType() {
            return MutableReference.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<MutableReference>() {
                @Override
                public int getTypeId() {
                    return MUTABLE_REFERENCE;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, MutableReference object) throws IOException {
                    out.writeObject(object.get());
                }

                @Override
                public MutableReference read(ObjectDataInput in) throws IOException {
                    return new MutableReference<>(in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LinTrendAccSerializer implements SerializerHook<LinTrendAccumulator> {

        @Override
        public Class<LinTrendAccumulator> getSerializationType() {
            return LinTrendAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LinTrendAccumulator>() {
                @Override
                public int getTypeId() {
                    return LIN_TREND_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LinTrendAccumulator object) throws IOException {
                    object.writeObject(out);
                }

                @Override
                public LinTrendAccumulator read(ObjectDataInput in) throws IOException {
                    return new LinTrendAccumulator(
                            in.readLong(), readBigInt(in), readBigInt(in), readBigInt(in), readBigInt(in));
                }

                private BigInteger readBigInt(ObjectDataInput in) throws IOException {
                    byte[] bytes = new byte[in.readUnsignedByte()];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = in.readByte();
                    }
                    return new BigInteger(bytes);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LongLongAccSerializer implements SerializerHook<LongLongAccumulator> {

        @Override
        public Class<LongLongAccumulator> getSerializationType() {
            return LongLongAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongLongAccumulator>() {
                @Override
                public int getTypeId() {
                    return LONG_LONG_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LongLongAccumulator object) throws IOException {
                    out.writeLong(object.getValue1());
                    out.writeLong(object.getValue2());
                }

                @Override
                public LongLongAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongLongAccumulator(in.readLong(), in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class LongDoubleAccSerializer implements SerializerHook<LongDoubleAccumulator> {

        @Override
        public Class<LongDoubleAccumulator> getSerializationType() {
            return LongDoubleAccumulator.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<LongDoubleAccumulator>() {
                @Override
                public int getTypeId() {
                    return LONG_DOUBLE_ACC;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, LongDoubleAccumulator object) throws IOException {
                    out.writeLong(object.getValue1());
                    out.writeDouble(object.getValue2());
                }

                @Override
                public LongDoubleAccumulator read(ObjectDataInput in) throws IOException {
                    return new LongDoubleAccumulator(in.readLong(), in.readDouble());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
