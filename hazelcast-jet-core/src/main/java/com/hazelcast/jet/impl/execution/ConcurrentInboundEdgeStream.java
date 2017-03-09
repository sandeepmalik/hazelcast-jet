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

import com.hazelcast.internal.util.concurrent.update.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.update.Pipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.function.Predicate;

import java.util.BitSet;
import java.util.Collection;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;

/**
 * {@code InboundEdgeStream} implemented in terms of a {@code ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker;
    private final BitSet wmReceived;
    private final PunctuationDetector wmDetector = new PunctuationDetector();
    private Punctuation currentPunc;

    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.tracker = new ProgressTracker();
        this.wmReceived = new BitSet(conveyor.queueCount());
    }

    /**
     * Drains all inbound queues into the {@code dest} collection. After
     * encountering a {@link Punctuation} in a particular queue, stops draining from
     * it and drains other queues until it receives {@link Object#equals(Object)
     * equal} punctuations from all of them. At that point it adds the punctuation to
     * the {@code dest} collection.
     * <p>
     * Receiving a non-equal punctuation produces an error. So does receiving a new
     * punctuation while some queue is already done or when a queue becomes done without
     * emitting the expected punctuation.
     */
    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        tracker.reset();
        for (int queueIndex = 0; queueIndex < conveyor.queueCount(); queueIndex++) {
            final Pipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }
            if (alreadyAtPunctuation(queueIndex)) {
                tracker.notDone();
                continue;
            }
            Punctuation punc = drainWithPunctuationDetection(q, dest);
            if (punc == null) {
                continue;
            }
            // we've got a punctuation, handle it
            validatePunctuation(punc);
            if (punc == DONE_ITEM) {
                conveyor.removeQueue(queueIndex);
                continue;
            }
            wmReceived.set(queueIndex);
            currentPunc = punc;
            if (allPunctuationsReceived()) {
                dest.add(currentPunc);
                currentPunc = null;
                wmReceived.clear();
                break;
            }
        }
        return tracker.toProgressState();
    }

    private boolean alreadyAtPunctuation(int i) {
        return currentPunc != null && wmReceived.get(i);
    }

    private boolean allPunctuationsReceived() {
        return wmReceived.nextClearBit(0) == conveyor.queueCount();
    }

    private void validatePunctuation(Punctuation punc) {
        if (currentPunc == null) {
            if (punc != DONE_ITEM && conveyor.liveQueueCount() < conveyor.queueCount()) {
                throw new JetException(
                        "Received a new punctuation after some processor already completed (punc=" + punc + ')');
            }
            return;
        }
        if (punc == DONE_ITEM) {
            throw new JetException("Processor completed without first emitting a punctuation" +
                    " that was already emitted by another processor (punc=" + currentPunc + ')');
        }
        if (!punc.equals(currentPunc)) {
            throw new JetException("Punctuation emitted by one processor not equal to punctuation emitted by "
                    + "another one, wm1=" + currentPunc + ", wm2=" + punc
                    + ", all processors must emit equal punctuations in the same order");
        }
    }

    /**
     * Drains the supplied queue into a {@code dest} collection, up
     * to the next {@link Punctuation}. Also updates the {@code tracker} with new
     * status.
     *
     * @return Punctuation, if found, or null
     */
    private Punctuation drainWithPunctuationDetection(Pipe<Object> queue, Collection<Object> dest) {
        wmDetector.dest = dest;
        wmDetector.punc = null;

        int drainedCount = queue.drain(wmDetector);
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, wmDetector.punc == DONE_ITEM));

        wmDetector.dest = null;
        return wmDetector.punc;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Punctuation}s.
     * When encountering a punctuation, prevents draining more items.
     */
    private static final class PunctuationDetector implements Predicate<Object> {
        Collection<Object> dest;
        Punctuation punc;

        @Override
        public boolean test(Object o) {
            if (o instanceof Punctuation) {
                assert punc == null : "Received a punctuation item, but this.punc != null";
                punc = (Punctuation) o;
                return false;
            }
            return dest.add(o);
        }
    }
}
