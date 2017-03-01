package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Watermark;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

final class CollectionWithWatermarkDetector extends AbstractCollection<Object> {

    Collection<Object> wrapped;
    Watermark watermark;

    @Override
    public Iterator<Object> iterator() {
        return wrapped.iterator();
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean add(Object o) {
        if (o instanceof Watermark) {
            watermark = (Watermark) o;
            return false;
        } else {
            return wrapped.add(o);
        }
    }
}