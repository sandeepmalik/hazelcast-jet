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
 * of a watermark item in the stream.
 */
public class WatermarkAwareProcessor extends AbstractProcessor {

    protected boolean tryProcessWm0(Watermark wm) {
        return tryProcessWm(0, wm);
    }

    protected boolean tryProcessWm1(Watermark wm) {
        return tryProcessWm(1, wm);
    }

    protected boolean tryProcessWm2(Watermark wm) {
        return tryProcessWm(2, wm);
    }

    protected boolean tryProcessWm3(Watermark wm) {
        return tryProcessWm(3, wm);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    protected boolean tryProcessWm4(Watermark wm) {
        return tryProcessWm(4, wm);
    }

    protected boolean tryProcessWm(int ordinal, Watermark wm) {
        throw new UnsupportedOperationException("Missing implementation");
    }


    @Override
    void process0(@Nonnull Inbox inbox) throws Exception {
        for (Object item; (item = inbox.peek()) != null; ) {
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm0((Watermark) item)
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
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm1((Watermark) item)
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
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm2((Watermark) item)
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
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm3((Watermark) item)
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
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm4((Watermark) item)
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
            final boolean doneWithItem = item instanceof Watermark
                    ? tryProcessWm(ordinal, (Watermark) item)
                    : tryProcess(ordinal, item);
            if (!doneWithItem) {
                return;
            }
            inbox.remove();
        }
    }
}
