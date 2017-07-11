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

package com.hazelcast.jet.connector.hbase.mapping;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.sort;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.beanutils.PropertyUtils.getPropertyDescriptors;
import static org.apache.hadoop.hbase.util.Pair.newPair;

/**
 * A light weight, simple mapping framework for HBase. It can convert a Java object to {@link HBaseRow}, which
 * then is converted to either {@link org.apache.hadoop.hbase.client.Put Put} operation or {@link Result} object
 * depending on whether the context is read or write.
 * <p>
 * This class is thread-safe. It scans a Java class for fields or methods that are annotated
 * with {@link Column} or {@link Id} annotations, and currently works with primitive
 * Java types and {@link String} objects
 * <p>
 * If HBase row key is required to be concatenation of multiple fields, then all those fields must be marked with
 * {@link Id} annotation. The ordinal property defines in which order the fields will be concatenated.
 * <p>
 * Finally, if some part of the row key is a {@link String}, then the length property must also be defined unless the
 * {@link String} part occurs at the end.
 * <p>
 * Usage example:
 * 1. For a row key consisting of a boolean, long, and String
 * public class MyRow {
 *
 * @Id(name = "myBool", ordinal = 0)
 * private boolean myBool;
 * @Id(name = "myLong", ordinal = 1)
 * private boolean myLong;
 * @Id(name = "myString", ordinal = 2)
 * private boolean myString;
 * }
 * <p>
 * 2. For a row key consisting of a boolean, String, and long
 * public class MyRow {
 * @Id(name = "myBool", ordinal = 0)
 * private boolean myBool;
 * @Id(name = "myString", ordinal = 1, length = 20)
 * private boolean myString;
 * @Id(name = "myLong", ordinal = 2)
 * private boolean myLong;
 */
public class HBaseObjectMapper {

    private static final Map<Class<?>, Map<Pair<String, String>, BiConsumer<byte[], ?>>> entities = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Map<Pair<String, String>, AccessibleObject>> entitiesReaders = new ConcurrentHashMap<>();
    private static final Map<Class<?>, List<AccessibleObject>> entitiesIdFields = new ConcurrentHashMap<>();

    public static void register(Class<?> entityClass) {
        if (entities.containsKey(entityClass))
            return;
        final Map<Pair<String, String>, BiConsumer<byte[], ?>> mappings = new HashMap<>();
        final List<AccessibleObject> idFields = new ArrayList<>();
        final Map<Pair<String, String>, AccessibleObject> readers = new HashMap<>();

        final Field[] declaredFields = entityClass.getDeclaredFields();
        AccessibleObject.setAccessible(declaredFields, true);
        final Map<String, Field> fields = stream(declaredFields).
                filter(f -> f.isAnnotationPresent(Id.class) || f.isAnnotationPresent(Column.class)).
                collect(toMap(Field::getName, identity()));

        stream(getPropertyDescriptors(entityClass)).forEach(pd -> {
            final Method setter = pd.getWriteMethod();
            if (setter != null && setter.isAnnotationPresent(Column.class)) {
                final Column a = setter.getAnnotation(Column.class);
                final Class<?> paramClass = setter.getParameterTypes()[0];
                mappings.put(newPair(a.family(), a.qualifier()), mapper(paramClass, setter));
                readers.put(newPair(a.family(), a.qualifier()), pd.getReadMethod());
            } else if (setter != null && setter.isAnnotationPresent(Id.class)) {
                final Id a = setter.getAnnotation(Id.class);
                final Class<?> paramClass = setter.getParameterTypes()[0];
                mappings.put(newPair(a.name(), null), mapper(paramClass, setter));
                readers.put(newPair(a.name(), null), pd.getReadMethod());
                idFields.add(setter);
            } else if (fields.containsKey(pd.getName())) {
                final Field f = fields.get(pd.getName());
                if (f.isAnnotationPresent(Column.class)) {
                    final Column a = f.getAnnotation(Column.class);
                    mappings.put(newPair(a.family(), a.qualifier()), mapper(f.getType(), f));
                    readers.put(newPair(a.family(), a.qualifier()), f);
                } else if (f.isAnnotationPresent(Id.class)) {
                    final Id a = f.getAnnotation(Id.class);
                    mappings.put(newPair(a.name(), null), mapper(f.getType(), f));
                    idFields.add(f);
                    readers.put(newPair(a.name(), null), f);
                }
            }
        });
        sort(idFields, (ao1, ao2) -> ao1.getAnnotation(Id.class).ordinal() - ao2.getAnnotation(Id.class).ordinal());
        entitiesIdFields.putIfAbsent(entityClass, idFields);
        entitiesReaders.putIfAbsent(entityClass, readers);
        entities.putIfAbsent(entityClass, mappings);
    }

    public static <T> T map(HBaseRow row, Class<T> entityClass) {
        try {
            final Map<Pair<String, String>, BiConsumer<byte[], ?>> mappings = entities.get(entityClass);
            if (mappings == null)
                throw new IllegalStateException("could not compute entity mapping: " + entityClass);
            final T t = entityClass.newInstance();
            mapRowKey(entitiesIdFields.get(entityClass), row.rowKey(), t);
            final Result result = (Result) row.unwrap();
            mappings.forEach((p, f) -> {
                Cell cell = null;
                if (p.getSecond() != null)
                    cell = result.getColumnLatestCell(Bytes.toBytes(p.getFirst()), Bytes.toBytes(p.getSecond()));
                if (cell != null) {
                    final byte[] value = CellUtil.cloneValue(cell);
                    @SuppressWarnings("unchecked")
                    final BiConsumer<byte[], T> dup = (BiConsumer<byte[], T>) f;
                    dup.accept(value, t);
                }
            });
            return t;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public static HBaseRow map(Result result) {
        final List<List<byte[]>> cells = new ArrayList<>();
        final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        map.forEach((cf, m) -> {
            final List<byte[]> row = new ArrayList<>();
            row.add(cf);
            m.forEach((q, m_) -> {
                row.add(q);
                m_.forEach((ts, v) -> {
                    row.add(v);
                    row.add(Bytes.toBytes(ts));
                });
            });
            cells.add(row);
        });
        return new HBaseRow() {
            @Override
            public byte[] rowKey() {
                return result.getRow();
            }

            @Override
            public List<List<byte[]>> cells() {
                return cells;
            }

            @Override
            public Object unwrap() {
                return result;
            }
        };
    }

    public static <T> HBaseRow map(T t) {
        try {
            final Map<Pair<String, String>, AccessibleObject> readers = entitiesReaders.get(t.getClass());
            if (readers == null)
                throw new IllegalStateException("could not compute entity mapping: " + t);
            final List<AccessibleObject> idFields = entitiesIdFields.get(t.getClass());
            final List<byte[]> idBytes = new ArrayList<>();
            for (AccessibleObject idField : idFields) {
                try {
                    final byte[] bytes = idField instanceof Method ? toBytes(((Method) idField).invoke(t)) :
                            toBytes(((Field) idField).get(t));
                    idBytes.add(bytes);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            final byte[] rowKey = com.google.common.primitives.Bytes.concat(idBytes.toArray(new byte[idBytes.size()][]));
            final List<List<byte[]>> cols = new ArrayList<>();
            for (Entry<Pair<String, String>, AccessibleObject> e : readers.entrySet()) {
                if (e.getKey().getSecond() == null) {
                    final byte[] family = Bytes.toBytes(e.getKey().getFirst());
                    final byte[] qualifier = Bytes.toBytes(e.getKey().getSecond());
                    final byte[] value = e.getValue() instanceof Method ? toBytes(((Method) e.getValue()).invoke(t)) :
                            toBytes(((Field) e.getValue()).get(t));
                    cols.add(asList(family, qualifier, value));
                }
            }
            return new HBaseRow() {
                @Override
                public byte[] rowKey() {
                    return rowKey;
                }

                @Override
                public List<List<byte[]>> cells() {
                    return cols;
                }

                @Override
                public Object unwrap() {
                    return t;
                }
            };
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static <T> BiConsumer<byte[], T> mapper(Class<?> paramClass, Method setter) {
        return (b, t) -> {
            try {
                setter.invoke(t, mapper0(paramClass).apply(b));
            } catch (Exception e) {
                throw rethrow(e);
            }
        };
    }

    private static <T> BiConsumer<byte[], T> mapper(Class<?> paramClass, Field f) {
        return (b, t) -> {
            try {
                f.set(t, mapper0(paramClass).apply(b));
            } catch (Exception e) {
                throw rethrow(e);
            }
        };
    }

    private static Function<byte[], ?> mapper0(Class<?> paramClass) {
        if (paramClass == int.class)
            return Bytes::toInt;
        if (paramClass == short.class)
            return Bytes::toShort;
        if (paramClass == long.class)
            return Bytes::toLong;
        if (paramClass == float.class)
            return Bytes::toFloat;
        if (paramClass == double.class)
            return Bytes::toDouble;
        if (paramClass == boolean.class)
            return Bytes::toBoolean;
        if (paramClass == String.class)
            return Bytes::toString;
        if (paramClass == byte[].class)
            return identity();
        throw new IllegalArgumentException("type not supported: " + paramClass);
    }

    private static byte[] toBytes(Object value) {
        if (value == null)
            return null;
        if (value instanceof Integer)
            return Bytes.toBytes((int) value);
        if (value instanceof Short)
            return Bytes.toBytes((short) value);
        if (value instanceof Byte)
            return Bytes.toBytes((byte) value);
        if (value instanceof Long)
            return Bytes.toBytes((long) value);
        if (value instanceof Float)
            return Bytes.toBytes((float) value);
        if (value instanceof Boolean)
            return Bytes.toBytes((boolean) value);
        if (value instanceof String)
            return Bytes.toBytes((String) value);
        if (value instanceof BigDecimal)
            return Bytes.toBytes((BigDecimal) value);
        throw new IllegalArgumentException("no known serialization: " + value);
    }

    private static void mapRowKey(List<AccessibleObject> idFields, byte[] bytes, final Object o) {
        final int[] offset = {0};
        idFields.forEach(ao -> {
            try {
                final Id a = ao.getAnnotation(Id.class);
                Class<?> paramClass = ao instanceof Field ? ((Field) ao).getType() : ((Method) ao).getParameterTypes()[0];
                byte[] subs = null;
                if (a.length() < 0) {
                    final int size = size(paramClass);
                    subs = Bytes.copy(bytes, offset[0], size);
                    offset[0] += size;
                } else if (paramClass == String.class || paramClass == byte[].class) {
                    if (a.length() < 0 || a.length() != MAX_VALUE)
                        throw new IllegalArgumentException("for string type, length must be explicitly set in the @Id annotation");
                    if (a.length() == MAX_VALUE) {
                        subs = Bytes.copy(bytes, offset[0], bytes.length - offset[0]);
                        offset[0] += (bytes.length - offset[0]);
                    } else {
                        subs = Bytes.copy(bytes, offset[0], a.length());
                        offset[0] += a.length();
                    }
                }
                if (subs == null)
                    throw new IllegalArgumentException("could not determine id key: " + idFields);
                if (ao instanceof Field) {
                    mapper(paramClass, ((Field) ao)).accept(subs, o);
                } else mapper(paramClass, ((Method) ao)).accept(subs, o);
            } catch (Exception e) {
                throw rethrow(e);
            }
        });
    }

    private static int size(Class<?> idClass) {
        if (idClass == int.class)
            return 4;
        if (idClass == short.class)
            return 2;
        if (idClass == long.class)
            return 8;
        if (idClass == float.class)
            return 4;
        if (idClass == double.class)
            return 8;
        if (idClass == boolean.class)
            return 1;
        throw new IllegalArgumentException("could not determine id size: " + idClass);
    }
}
