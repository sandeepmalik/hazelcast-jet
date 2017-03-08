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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.execution.DoneWatermark.DONE_WM;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class ProcessorTasklet implements Tasklet {

    private final ArrayDequeInbox inbox = new ArrayDequeInbox();
    private final ProgressTracker progTracker = new ProgressTracker();
    private final Processor processor;
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private final String vertexName;
    private final Context context;
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private final ArrayDequeOutbox outbox;
    private final OutboundEdgeStream[] outstreams;

    private InboundEdgeStream currInstream;
    private boolean currInstreamExhausted;
    private boolean processorCompleted;

    public ProcessorTasklet(String vertexName, Processor.Context context, Processor processor,
                            List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams) {
        Preconditions.checkNotNull(processor, "processor");
        this.vertexName = vertexName;
        this.processor = processor;
        this.instreamGroupQueue = instreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new, toCollection(ArrayList::new)))
                .entrySet().stream()
                .map(Entry::getValue)
                .collect(toCollection(ArrayDeque::new));

        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);

        int[] outboxLimits = Stream.of(this.outstreams).mapToInt(OutboundEdgeStream::getOutboxLimit).toArray();
        this.outbox = new ArrayDequeOutbox(outstreams.size(), outboxLimits);
        this.context = context;
        this.instreamCursor = popInstreamGroup();
    }


    @Override
    public void init() {
        processor.init(outbox, context);
    }

    @Override
    public boolean isCooperative() {
        return processor.isCooperative();
    }

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();
        tryFillInbox();
        outbox.resetDidAdd();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox.isEmpty()) {
            tryProcessInbox();
        } else if (currInstreamExhausted) {
            if (processor.completeEdge(currInstream.ordinal())) {
                currInstream = null;
            }
        } else {
            processor.process();
        }
        tryFlushOutbox();
        progTracker.madeProgress(outbox.didAdd());
        return progTracker.toProgressState();
    }

    private CircularListCursor<InboundEdgeStream> popInstreamGroup() {
        return Optional.ofNullable(instreamGroupQueue.poll()).map(CircularListCursor::new).orElse(null);
    }

    private void tryFillInbox() {
        // we have more items in inbox, or current inbound stream is exhausted but its processing hasn't completed
        if (!inbox.isEmpty() || currInstream != null && currInstreamExhausted) {
            progTracker.notDone();
            return;
        }
        if (instreamCursor == null) {
            return;
        }
        progTracker.notDone();
        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            currInstream = instreamCursor.value();
            result = currInstream.drainTo(inbox);
            progTracker.madeProgress(result.isMadeProgress());
            currInstreamExhausted = result.isDone();
            if (currInstreamExhausted) {
                instreamCursor.remove();
            }
            if (!instreamCursor.advance()) {
                instreamCursor = popInstreamGroup();
                return;
            }
        } while (!result.isMadeProgress() && instreamCursor.value() != first);
    }

    private void tryProcessInbox() {
        if (outbox.hasReachedLimit()) {
            progTracker.notDone();
            return;
        }
        final int inboundOrdinal = currInstream.ordinal();
        final Watermark wm = inbox.peekWatermark();
        if (wm != null) {
            if (processor.tryProcessWatermark(inboundOrdinal, wm)) {
                inbox.removeWatermark();
            }
        } else {
            processor.process(inboundOrdinal, inbox);
        }
        if (!inbox.isEmpty()) {
            progTracker.notDone();
        }
    }

    private void completeIfNeeded() {
        if (processorCompleted) {
            return;
        }
        if (outbox.hasReachedLimit()) {
            progTracker.notDone();
            return;
        }
        processorCompleted = processor.complete();
        if (processorCompleted) {
            for (OutboundEdgeStream outstream : outstreams) {
                outbox.add(outstream.ordinal(), DONE_WM);
            }
            return;
        }
        progTracker.notDone();
    }

    private void tryFlushOutbox() {
        nextOutstream:
        for (int i = 0; i < outbox.bucketCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                final OutboundCollector c = outstreams[i].getCollector();
                final ProgressState state =
                        (item instanceof Watermark ? c.offerWatermark((Watermark) item) : c.offer(item));
                progTracker.madeProgress(state.isMadeProgress());
                if (!state.isDone()) {
                    progTracker.notDone();
                    continue nextOutstream;
                }
                q.remove();
            }
        }
    }

    @Override
    public String toString() {
        return "ProcessorTasklet{vertex=" + vertexName + ", processor=" + processor + '}';
    }
}

