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

package com.hazelcast.jet.impl.util;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static com.hazelcast.jet.impl.util.Util.indexOf;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Helper class to implement the logic to reduce skew. Intended usage is:<ul>
 *     <li>Create an instance with number of your queues</li>
 *     <li>Drain your queues in the order as specified by {@link #realQueueIndex(int)}</li>
 *     <li>Whenever you receive a {@link com.hazelcast.jet.Punctuation} from one of the
 *     queues, call {@link #observePunc(int, long)}</li>
 *     <li>After draining single queue, check the result of {@link #shouldDrainQueue(int, boolean)}
 *     to see, whether to continue with the next queue.</li>
 * </ul>
 */
public class SkewReductionPolicy {

    /**
     * <i>Skew</i> is the maximum of difference between top punctuation from two queues.
     *
     * <pre>
     * +-----------+------------+-----------+---------------+------------+
     * |  Method   | Drops data |  Latency  | Allowed skew* | Throughput |
     * +-----------+------------+-----------+---------------+------------+
     * | NO_ACTION | No         | Unbounded | Unbounded     | High       |
     * | WAIT      | No         | Unbounded | Bounded       | Reduced    |
     * | SKIP      | Yes        | Bounded   | Bounded       | High       |
     * +-----------+------------+-----------+---------------+------------+
     * </pre>
     * (*) Higher allowed skew causes higher memory usage due to more window frames that
     * need to be kept open.
     */
    public enum SkewExceededAction {

        /**
         * If skew is exceeded, stop draining from queues, that are more than {@code maxSkew}
         * ahead.
         * <p>
         * Ensures the skew never exceeds {@code maxSkew} and decreases throughput
         * by waiting for stalled queues. Useful, if the memory needed to store and snapshot
         * additional open frames is large and we strictly need to limit it. Increases latency.
         * <p>
         * Might cause deadlock, if the drained and the not-drained queues are populated
         * from single upstream source.
         */
        WAIT,

        /**
         * We'll emit punctuation with time (topPunct - maxSkew), despite the fact, that
         * some queues did not yet arrive at it.
         * <p>
         * Causes incorrectness, but guarantees minimum latency and bounded skew.
         */
        SKIP,

        /**
         * No special action is taken, just continue draining with priority.
         */
        NO_ACTION
    }

    // package-visible for tests
    final long[] observedPuncSeqs;
    final int[] orderedQueues;

    private final long maxSkew;
    private final long applyPriorityThreshold;
    private final SkewExceededAction skewExceededAction;

    public SkewReductionPolicy(int numQueues, long maxSkew, long applyPriorityThreshold,
            @Nonnull SkewExceededAction skewExceededAction) {
        checkNotNegative(maxSkew, "maxSkew must be >= 0");
        checkNotNegative(applyPriorityThreshold, "applyPriorityThreshold must be >= 0");
        checkTrue(applyPriorityThreshold <= maxSkew, "applyPriorityThreshold must be less than maxSkew");

        this.maxSkew = maxSkew;
        this.applyPriorityThreshold = applyPriorityThreshold;
        this.skewExceededAction = skewExceededAction;

        observedPuncSeqs = new long[numQueues];
        Arrays.fill(observedPuncSeqs, Long.MIN_VALUE);

        orderedQueues = new int[numQueues];
        Arrays.setAll(orderedQueues, i -> i);
    }

    /**
     * Process {@link com.hazelcast.jet.Punctuation} from queue at {@code realQueueIndex}.
     *
     * @return true, if the queues were reordered by this punctuation.
     */
    public boolean observePunc(int realQueueIndex, final long puncSeq) {
        if (observedPuncSeqs[realQueueIndex] >= puncSeq) {
            // this is possible with SKIP scenario, where we increase observedPuncSeqs value
            // without receiving punct from that queue
            if (skewExceededAction != SkewExceededAction.SKIP) {
                throw new AssertionError("Punctuations not monotonically increasing on queue");
            }
            return false;
        }

        boolean res = reorderQueues(realQueueIndex, puncSeq);
        observedPuncSeqs[realQueueIndex] = puncSeq;

        // if action is SKIP, let's advance queues, that are too much behind, to topQueuePunc - maxSkew
        if (skewExceededAction == SkewExceededAction.SKIP) {
            long topQueuePunc = topObservedPunc();
            // this is to avoid integer overflow at the beginning
            if (topQueuePunc > Long.MIN_VALUE + maxSkew) {
                long newBottomQueuePunc = topQueuePunc - maxSkew;
                for (int i = 0; i < orderedQueues.length && observedPuncSeqs[orderedQueues[i]] < newBottomQueuePunc; i++) {
                    observedPuncSeqs[orderedQueues[i]] = newBottomQueuePunc;
                }
            }
        }

        return res;
    }

    /**
     * Returns queue index for queue, that should be drained as n-th.
     * For example, {@code orderedQueueIndex==0} returns the queue, that is most behind etc.
     */
    public int realQueueIndex(int orderedQueueIndex) {
        return orderedQueues[orderedQueueIndex];
    }

    public boolean shouldDrainQueue(int orderedQueueIndex, boolean madeProgress) {
        // always drain the first queue
        if (orderedQueueIndex == 0) {
            return true;
        }

        long thisQueuePunc = observedPuncSeqs[orderedQueues[orderedQueueIndex]];
        long bottomQueuePunc = observedPuncSeqs[orderedQueues[0]];

        // always drain queues, that are less than applyPriorityThreshold ahead
        if (thisQueuePunc <= bottomQueuePunc + applyPriorityThreshold) {
            return true;
        }

        // next queue is too much ahead. If we made progress so far, let's stop draining
        if (madeProgress) {
            return false;
        }

        // if policy is to WAIT and the next queue is more than maxSkew ahead, don't drain it
        if (skewExceededAction == SkewExceededAction.WAIT && skewIsMoreThanMax(bottomQueuePunc, thisQueuePunc)) {
            return false;
        }

        // otherwise return true
        return true;
    }

    public long bottomObservedPunc() {
        return observedPuncSeqs[orderedQueues[0]];
    }

    public long topObservedPunc() {
        return observedPuncSeqs[orderedQueues[orderedQueues.length - 1]];
    }

    private boolean reorderQueues(int queueIndex, long puncSeq) {
        // Reorder the queues from the most behind one to the most ahead one. They are ordered now,
        // and the one at queueIndex just has advanced.
        int currentPos = indexOf(orderedQueues, queueIndex);
        int newPos = lowerBoundQueuePosition(puncSeq) - 1;
        // the queue position must always go closer to the end, as it's current punc is higher
        assert newPos >= currentPos;
        if (newPos > currentPos && currentPos < orderedQueues.length - 1) {
            System.arraycopy(orderedQueues, currentPos + 1, orderedQueues, currentPos, newPos - currentPos);
            orderedQueues[newPos] = queueIndex;
            return true;
        } else {
            return false;
        }
    }

    private int lowerBoundQueuePosition(long key) {
        int low = 0;
        int high = observedPuncSeqs.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = observedPuncSeqs[orderedQueues[mid]];

            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return low;  // key not found.
    }

    boolean skewIsMoreThanMax(long behindPunc, long aheadPunc) {
        assert behindPunc <= aheadPunc;
        // We don't expect the behindPunc to get close to Long.MAX_VALUE (a.k.a. the end of history),
        // however, we expect the behindPunc to be Long.MIN_VALUE.
        return aheadPunc > behindPunc + maxSkew;
    }

    public long getMaxSkew() {
        return maxSkew;
    }

}
