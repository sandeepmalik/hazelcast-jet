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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Traversers.ResettableSingletonTraverser;

import javax.annotation.Nonnull;

/**
 * A processor that inserts punctuation into a data stream. See
 * {@link WindowingProcessors#insertPunctuation(ToLongFunction, Supplier)
 * WindowingProcessors.insertPunctuation()} for documentation.
 *
 * @param <T> input event type
 */
public class InsertPunctuationP<T> extends AbstractProcessor {

    private final ToLongFunction<T> extractEventSeqF;
    private final PunctuationKeeper punctuationKeeper;
    private final ResettableSingletonTraverser<Object> singletonTraverser;
    private final Traverser<Object> nullTraverser = Traversers.newNullTraverser();
    private final FlatMapper<Object, Object> flatMapper;

    private long currPunc = Long.MIN_VALUE;

    /**
     * @param extractEventSeqF function that extracts the {@code eventSeq} from an input item
     * @param punctuationKeeper the punctuation keeper
     */
    InsertPunctuationP(@Nonnull ToLongFunction<T> extractEventSeqF,
                       @Nonnull PunctuationKeeper punctuationKeeper
    ) {
        this.extractEventSeqF = extractEventSeqF;
        this.punctuationKeeper = punctuationKeeper;
        this.flatMapper = flatMapper(this::traverser);
        this.singletonTraverser = new ResettableSingletonTraverser<>();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        punctuationKeeper.init(context.nanoClock());
    }

    @Override
    public boolean tryProcess() {
        long newPunc = punctuationKeeper.getCurrentPunctuation();
        if (newPunc > currPunc) {
            boolean didEmit = tryEmit(new Punctuation(newPunc));
            if (didEmit) {
                currPunc = newPunc;
            }
            return didEmit;
        }
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return flatMapper.tryProcess(item);
    }

    private Traverser<Object> traverser(Object item) {
        long eventSeq = extractEventSeqF.applyAsLong((T) item);
        if (eventSeq < currPunc) {
            // drop late event
            return nullTraverser;
        }
        long newPunc = punctuationKeeper.reportEvent(eventSeq);
        singletonTraverser.accept(item);
        if (newPunc > currPunc) {
            currPunc = newPunc;
            return singletonTraverser.prepend(new Punctuation(currPunc));
        }
        return singletonTraverser;
    }
}
