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

import static com.hazelcast.jet.impl.util.SkewReductionPolicy.SkewExceededAction.SKIP;
import static com.hazelcast.jet.impl.util.SkewReductionPolicy.SkewExceededAction.WAIT;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * <i>Stream skew</i> is defined on a set of its substreams that are being
 * consumed in parallel: given the set of the punctuation values on each
 * substream, it is the difference between the top and the bottom value
 * in that set. Stream skew has negative effects on at least one of:
 * <ol><li>
 *     memory consumption
 * </li><li>
 *     throughput
 * </li><li>
 *     latency
 * </li><li>
 *     correctness
 * </li></ol>
 * This class implements a policy to reduce the stream skew by managing the
 * priority of draining individual substream queues. As soon as a queue's
 * punctuation advances above the configured {@code
 * priorityDrainingThreshold}, the priority of draining that queue will be
 * lowered so that all other queues are drained before it. Since all queues
 *
 *
 * The intended usage is
 * as follows:
 * <ol><li>
 *     Create an instance of this class, supplying the number of queues whose
 *     draining order must be managed.
 * </li><li>
 *     Use an indexed loop ranging over {@code [0..numQueues)}.
 * </li><li>
 *     For each {@code i} drain the queue indicated by {@link #toQueueIndex(
 *     int)}.
 * </li><li>
 *     Call {@link #observePunc(int, long) observePunc(realIndex, puncSeq)} for
 *     every punctuation item received from any queue.
 * </li><li>
 *     After draining one queue, check the result of {@link #shouldStopDraining(
 *     int, boolean) shouldDrainQueue(drainIndex, madeProgress)} to see
 *     whether to continue with the next queue.
 * </li></ol>
 */
public class SkewReductionPolicy {

    /**
     * Enum providing a choice of actions to take when the stream skew has
     * exceeded the configured limit.
     * <pre>
     * +-----------+------------+-----------+---------------+------------+
     * |  Method   | Drops data |  Latency  | Allowed skew* | Throughput |
     * +-----------+------------+-----------+---------------+------------+
     * | NO_ACTION | No         | Unbounded | Unbounded     | High       |
     * | WAIT      | No         | Unbounded | Bounded       | Reduced    |
     * | SKIP      | Yes        | Bounded   | Bounded       | High       |
     * +-----------+------------+-----------+---------------+------------+
     * </pre>
     * (*) Higher allowed skew causes higher memory usage by causing more data
     * to be retained in a vertex that performs a windowing operation.
     */
    public enum SkewExceededAction {

        /**
         * Stop draining the queues that are more than {@code maxSkew} ahead of the
         * least advanced queue.
         * <p>
         * This ensures that the skew never exceeds {@code maxSkew}, thereby
         * limiting memory consumption, but degrading throughput and latency.
         * <p>
         * Might cause a deadlock if the drained and the not-drained queues are
         * being populated from a single upstream source.
         */
        WAIT,

        /**
         * Never cease draining any queues, but force the punctuation to stay
         * within {@code (topPunc - maxSkew)}. Data from the queues that fall too
         * much behind will be dropped as "late events".
         * <p>
         * Causes incorrectness, but guarantees minimum latency and bounded skew.
         */
        SKIP,

        /**
         * Don't cease draining any queues and don't force-advance the punctuation
         * either; instead let the stream skew exceed the configured limit. Even
         * though all the data is processed as soon as possible, latency can grow
         * without bounds due to the lagging punctuation. This will also cause
         * memory consumption to rise as the system keeps buffering all the data
         * ahead of the punctuation.
         */
        NO_ACTION
    }

    // package-visible for tests
    final long[] queuePuncSeqs;
    final int[] drainOrderToQIdx;

    private final long maxSkew;
    private final long priorityDrainingThreshold;
    private final SkewExceededAction skewExceededAction;

    public SkewReductionPolicy(int numQueues, long maxSkew, long priorityDrainingThreshold,
            @Nonnull SkewExceededAction skewExceededAction
    ) {
        checkNotNegative(maxSkew, "maxSkew must not be a negative number");
        checkNotNegative(priorityDrainingThreshold, "priorityDrainingThreshold must not be a negative number");
        checkTrue(priorityDrainingThreshold <= maxSkew, "priorityDrainingThreshold must be less than maxSkew");

        this.maxSkew = maxSkew;
        this.priorityDrainingThreshold = priorityDrainingThreshold;
        this.skewExceededAction = skewExceededAction;

        queuePuncSeqs = new long[numQueues];
        Arrays.fill(queuePuncSeqs, Long.MIN_VALUE);

        drainOrderToQIdx = new int[numQueues];
        Arrays.setAll(drainOrderToQIdx, i -> i);
    }

    /**
     * Given the (variable) position of a queue in the draining order, returns
     * the (fixed) index of that queue in the array of all queues. Queues are
     * ordered for draining by their punctuation (lowest first) so that the
     * least advanced queue is drained first.
     */
    public int toQueueIndex(int drainOrderIdx) {
        return drainOrderToQIdx[drainOrderIdx];
    }

    /**
     * Called to report the value of punctuation observed on the queue at
     * {@code queueIndex}.
     *
     * @return {@code true} if the queues were reordered by this punctuation
     */
    public boolean observePunc(int queueIndex, final long puncSeq) {
        if (queuePuncSeqs[queueIndex] >= puncSeq) {
            // this is possible with SKIP scenario, where we increase observedPuncSeqs
            // without receiving punctuation from that queue
            if (skewExceededAction != SKIP) {
                throw new AssertionError("Punctuations not monotonically increasing on queue");
            }
            return false;
        }
        boolean res = reorderQueues(queueIndex, puncSeq);
        queuePuncSeqs[queueIndex] = puncSeq;
        if (skewExceededAction != SKIP) {
            return res;
        }
        // The configured action is SKIP, so ensure that all queues' punctuation is
        // at least topObservedPunc - maxSkew
        long topPunc = topObservedPunc();
        // prevent possible integer overflow
        if (topPunc <= Long.MIN_VALUE + maxSkew) {
            return res;
        }
        long newBottomPunc = topPunc - maxSkew;
        for (int i = 0; i < drainOrderToQIdx.length && queuePuncSeqs[drainOrderToQIdx[i]] < newBottomPunc; i++) {
            queuePuncSeqs[drainOrderToQIdx[i]] = newBottomPunc;
        }
        return res;
    }

    /**
     * The queue-draining loop drains the queues in the drain order specified
     * by this class and consults this method before going on to drain
     * the next queue. To determine the response, we determine the skew of the
     * queue the loop is about to drain: it is the difference between its
     * punctuation and the bottom punctuation (i.e., that of the first queue in
     * the draining order). The policy will signal to stop draining if:
     * <ol><li>
     *     some data was already drained and the queue's skew is above the
     *     configured "priority draining" threshold, or
     * </li><li>
     *     the queue's skew is above the configured {@code maxSkew} and the
     *     action to take on exceeding the limit is {@link
     *     SkewExceededAction#WAIT WAIT}.
     * </li></ol>
     *
     * @param drainOrderIdx the current position in the draining order
     * @param madeProgress whether any queue was drained so far
     * @return {@code false} if the draining should now stop; {@code true} otherwise
     */
    public boolean shouldStopDraining(int drainOrderIdx, boolean madeProgress) {
        // Don't calculade the punc difference directly to avoid integer overflow
        long thisQueuePunc = queuePuncSeqs[drainOrderToQIdx[drainOrderIdx]];
        long bottomPunc = queuePuncSeqs[drainOrderToQIdx[0]];
        return (madeProgress && thisQueuePunc > bottomPunc + priorityDrainingThreshold)
                || (skewExceededAction == WAIT && thisQueuePunc > bottomPunc + maxSkew);
    }

    public long bottomObservedPunc() {
        return queuePuncSeqs[drainOrderToQIdx[0]];
    }

    private long topObservedPunc() {
        return queuePuncSeqs[drainOrderToQIdx[drainOrderToQIdx.length - 1]];
    }

    private boolean reorderQueues(int queueIndex, long puncSeq) {
        // Reorder the queues from the least to the most advanced one in terms of
        // their punctuation. They are ordered now, and the one at `queueIndex` has
        // just advanced to `puncSeq`.
        int currentPos = indexOf(drainOrderToQIdx, queueIndex);
        int newPos = lowerBoundQueuePosition(puncSeq) - 1;
        // the queue position must always go closer to the end, as it's current punc is higher
        assert newPos >= currentPos;
        if (newPos > currentPos && currentPos < drainOrderToQIdx.length - 1) {
            System.arraycopy(drainOrderToQIdx, currentPos + 1, drainOrderToQIdx, currentPos, newPos - currentPos);
            drainOrderToQIdx[newPos] = queueIndex;
            return true;
        } else {
            return false;
        }
    }

    private int lowerBoundQueuePosition(long key) {
        int low = 0;
        int high = queuePuncSeqs.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = queuePuncSeqs[drainOrderToQIdx[mid]];

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

    private static int indexOf(int[] haystack, int needle) {
        for (int i = 0; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }
}
