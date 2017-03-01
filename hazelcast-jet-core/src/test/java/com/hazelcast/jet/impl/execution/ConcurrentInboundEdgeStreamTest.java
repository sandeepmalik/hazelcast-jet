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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.windowing.example.WatermarkWithTime;
import com.hazelcast.util.function.Predicate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static org.junit.Assert.assertEquals;

public class ConcurrentInboundEdgeStreamTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_twoEmittersOneDoneFirst_then_madeProgress() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 1);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 2);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        // emitter1 returned 1 and 2; emitter2 returned 6
        // emitter1 is now done
        assertEquals(Arrays.asList(1, 2, 6), list);
        assertEquals(ProgressState.MADE_PROGRESS, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        // emitter2 returned 7 and now both emitters are done
        assertEquals(Collections.singletonList(7), list);
        assertEquals(DONE, progressState);

        // both emitters are now done and made no progress since last call
        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    @Test
    public void when_twoEmittersDrainedAtOnce_then_firstCallDone() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 1);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        // emitter1 returned 1 and 2; emitter2 returned 6
        // both are now done
        assertEquals(Arrays.asList(1, 2, 6), list);
        assertEquals(DONE, progressState);
    }

    @Test
    public void when_allEmittersInitiallyDone_then_firstCallDone() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 0);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 0);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    @Test
    public void when_oneEmitterWithNoProgress_then_noProgress() {
        NoProgressEmitter emitter1 = new NoProgressEmitter();
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(1, 1, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new InboundEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        assertEquals(Collections.singletonList(1), list);
        assertEquals(ProgressState.MADE_PROGRESS, progressState);
        // now emitter2 is done, emitter1 is not but has no progress
        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.NO_PROGRESS, progressState);

        // now make emitter1 done, without returning anything
        emitter1.done = true;

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    @Test
    public void when_watermarkFromAllEmittersInSingleDrain_then_emitAtWm() {
        IntegerSequenceWithWatermarksEmitter emitter1 = new IntegerSequenceWithWatermarksEmitter(0, 3, 1);
        IntegerSequenceWithWatermarksEmitter emitter2 = new IntegerSequenceWithWatermarksEmitter(0, 3, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new InboundEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();

        ProgressState progressState = stream.drainTo(list);
        assertEquals(Arrays.asList(0, 1, 0, 1, new WatermarkWithTime(1)), list);
        assertEquals(MADE_PROGRESS, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(Arrays.asList(2, 2), list);
        assertEquals(DONE, progressState);
    }

    @Test
    public void when_watermarkFromSomeEmitter_then_dontEmit() {
        // This one will emit: [0, 1, WM(1)], [2]
        IntegerSequenceWithWatermarksEmitter emitter1 = new IntegerSequenceWithWatermarksEmitter(0, 3, 1);
        // This one will emit: [3, 4], [5, 6]
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(3, 2, 2);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new InboundEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();

        ProgressState progressState = stream.drainTo(list);
        assertEquals(Arrays.asList(0, 1, 3, 4), list);
        assertEquals(MADE_PROGRESS, progressState);

        list.clear();
        emitter2.wmAfterNextDrain = new WatermarkWithTime(1);
        progressState = stream.drainTo(list);
        assertEquals(Arrays.asList(5, 6, new WatermarkWithTime(1)), list);
        assertEquals(MADE_PROGRESS, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(Collections.singletonList(2), list);
        assertEquals(DONE, progressState);
    }

    @Test
    public void when_watermarkDontMatch_then_error() {
        IntegerSequenceWithWatermarksEmitter emitter1 = new IntegerSequenceWithWatermarksEmitter(0, 3, 0);
        IntegerSequenceWithWatermarksEmitter emitter2 = new IntegerSequenceWithWatermarksEmitter(0, 3, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new InboundEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        exception.expect(JetException.class);
        exception.expectMessage("Watermark");
        ProgressState progressState = stream.drainTo(list);
    }

    private static final class IntegerSequenceWithWatermarksEmitter implements InboundEmitter {

        private int currentValue;
        private final int endValue;
        private final int[] watermarksAfter;
        private int watermarkPos;

        /**
         * @param startAt Value to start at
         * @param endValue Value to end before
         * @param watermarksAfter After this values, there will be a watermark
         */
        IntegerSequenceWithWatermarksEmitter(int startAt, int endValue, int ... watermarksAfter) {
            currentValue = startAt;
            this.endValue = endValue;
            this.watermarksAfter = watermarksAfter;
            Arrays.sort(watermarksAfter); // for sure
        }

        @Override
        public ProgressState drain(Predicate<Object> itemHandler) {
            boolean shouldContinue = true;
            while (shouldContinue && currentValue < endValue) {
                if (watermarkPos < watermarksAfter.length && watermarksAfter[watermarkPos] < currentValue) {
                    shouldContinue = itemHandler.test(new WatermarkWithTime(watermarksAfter[watermarkPos]));
                    watermarkPos++;
                }
                else {
                    shouldContinue = itemHandler.test(currentValue++);
                }
            }

            return ProgressState.valueOf(true, currentValue >= endValue);
        }
    }

    private static final class IntegerSequenceEmitter implements InboundEmitter {

        private int currentValue;
        private int drainSize;
        private int endValue;

        private Watermark wmAfterNextDrain;

        /**
         * @param startAt Value to start at
         * @param drainSize Number of items to emit in one {@code drainTo} call
         * @param drainCallsUntilDone Total number of {@code drainTo} Calls until done
         */
        IntegerSequenceEmitter(int startAt, int drainSize, int drainCallsUntilDone) {
            currentValue = startAt;
            this.drainSize = drainSize;
            this.endValue = startAt + drainCallsUntilDone * drainSize;
        }

        @Override
        public ProgressState drain(Predicate<Object> itemHandler) {
            int i;
            for (i = 0; i < drainSize && currentValue < endValue; i++, currentValue++)
                if ( ! itemHandler.test(currentValue))
                    break;

            if (wmAfterNextDrain != null) {
                itemHandler.test(wmAfterNextDrain);
                wmAfterNextDrain = null;
            }

            return ProgressState.valueOf(i > 0, currentValue >= endValue);
        }
    }

    private static final class NoProgressEmitter implements InboundEmitter {

        boolean done;

        @Override
        public ProgressState drain(Predicate<Object> itemHandler) {
            return done ? ProgressState.WAS_ALREADY_DONE : ProgressState.NO_PROGRESS;
        }
    }

}
