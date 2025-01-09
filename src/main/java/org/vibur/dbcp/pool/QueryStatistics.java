/**
 * Copyright 2025 Simeon Malchev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vibur.dbcp.pool;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Query statistics implementation based on AtomicLong(s).
 */
public class QueryStatistics implements LongConsumer {

    private final AtomicLong exceptionsCount = new AtomicLong(0);
    private final AtomicLong queriesCount = new AtomicLong(0);
    private final AtomicLong nanoSum = new AtomicLong(0);
    private final AtomicLong nanoMin = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong nanoMax = new AtomicLong(Long.MIN_VALUE);

    /**
     * Records a new query execution time represented in <i>nanoseconds</i> into the statistics.
     *
     * @param nanoseconds the query time
     */
    @Override
    public void accept(long nanoTime) {
        queriesCount.incrementAndGet();
        nanoSum.addAndGet(nanoTime);
        while (nanoTime < nanoMin.get()) {
            nanoMin.getAndSet(nanoTime);
        }
        while (nanoTime > nanoMax.get()) {
            nanoMax.getAndSet(nanoTime);
        }
    }

    /**
     * Increment the currently recorded number of exceptions.
     */
    public void incrementExceptions() {
        exceptionsCount.incrementAndGet();
    }

    /**
     * Combines the state of another {@code QueryStatistics} into this one.
     *
     * @param other another {@code QueryStatistics}
     * @throws NullPointerException if {@code other} is null
     */
    public void combine(QueryStatistics other) {
        exceptionsCount.addAndGet(other.getExceptionsCount());
        queriesCount.addAndGet(other.getQueriesCount());
        nanoSum.addAndGet(other.getSum().toNanos());
        while (other.getMin().toNanos() < nanoMin.get()) {
            nanoMin.getAndSet(other.getMin().toNanos());
        }
        while (other.getMax().toNanos() > nanoMax.get()) {
            nanoMax.getAndSet(other.getMax().toNanos());
        }
    }

    /**
     * Resets the statistics to their initial values.
     */
    public void reset() {
        while (exceptionsCount.get() != 0 || queriesCount.get() != 0 || nanoSum.get() != 0
                || nanoMin.get() != Long.MAX_VALUE || nanoMax.get() != Long.MIN_VALUE) {
            exceptionsCount.getAndSet(0);
            queriesCount.getAndSet(0);
            nanoSum.getAndSet(0);
            nanoMin.getAndSet(Long.MAX_VALUE);
            nanoMax.getAndSet(Long.MIN_VALUE);
        }
    }

    /**
     * @return the count of exceptions collected in the statistics
     */
    public long getExceptionsCount() {
        return exceptionsCount.get();
    }

    /**
     * @return the count of queries collected in the statistics
     */
    public long getQueriesCount() {
        return queriesCount.get();
    }

    /**
     * @return the total execution time of all queries collected in the statistics
     */
    public Duration getSum() {
        return Duration.ofNanos(nanoSum.get());
    }

    /**
     * @return the minimum execution time from all queries collected in the statistics
     */
    public Duration getMin() {
        return Duration.ofNanos(nanoMin.get());
    }

    /**
     * @return the maximum execution time from all queries collected in the statistics
     */
    public Duration getMax() {
        return Duration.ofNanos(nanoMax.get());
    }

    /**
     * @return the arithmetic mean of all query execution times in <i>nanoseconds</i>, or zero if no values have been recorded
     */
    public double getAverage() {
        var count = queriesCount.get();
        return count > 0 ? (double) getSum().toNanos() / count : 0.0d;
    }

    @Override
    public String toString() {
        return String.format("{exceptions = %d, queries = %d, sum = %f ms, min = %f ms, average = %f ms, max = %f ms}",
                getExceptionsCount(), getQueriesCount(),
                getSum().toNanos() * 1e-6,
                getMin().toNanos() * 1e-6,
                getAverage() * 1e-6,
                getMax().toNanos() * 1e-6);
    }
}
