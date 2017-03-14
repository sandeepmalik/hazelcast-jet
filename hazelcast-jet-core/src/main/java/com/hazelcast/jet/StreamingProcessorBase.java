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

package com.hazelcast.jet;

import javax.annotation.Nonnull;

/**
 * Base class to implement a streaming processor that responds to the occurrence
 * of a punctuation item in the stream.
 */
public class StreamingProcessorBase extends AbstractProcessor {

    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        return tryProcessPunc(0, punc);
    }

    protected boolean tryProcessPunc1(@Nonnull Punctuation punc) {
        return tryProcessPunc(1, punc);
    }

    protected boolean tryProcessPunc2(@Nonnull Punctuation punc) {
        return tryProcessPunc(2, punc);
    }

    protected boolean tryProcessPunc3(@Nonnull Punctuation punc) {
        return tryProcessPunc(3, punc);
    }

    protected boolean tryProcessPunc4(@Nonnull Punctuation punc) {
        return tryProcessPunc(4, punc);
    }

    protected boolean tryProcessPunc(int ordinal, @Nonnull Punctuation punc) {
        throw new UnsupportedOperationException("Missing implementation");
    }


    @Override
    void process0(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc0((Punctuation) item)
                    : tryProcess0(item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }

    @Override
    void process1(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc1((Punctuation) item)
                    : tryProcess1(item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }

    @Override
    void process2(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc2((Punctuation) item)
                    : tryProcess2(item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }

    @Override
    void process3(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc3((Punctuation) item)
                    : tryProcess3(item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }

    @Override
    void process4(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc4((Punctuation) item)
                    : tryProcess4(item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }

    @Override
    void processAny(int ordinal, @Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Punctuation
                    ? tryProcessPunc(ordinal, (Punctuation) item)
                    : tryProcess(ordinal, item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }
}
