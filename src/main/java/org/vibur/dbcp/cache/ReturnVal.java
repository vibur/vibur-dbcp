/**
 * Copyright 2013 Simeon Malchev
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

package org.vibur.dbcp.cache;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thin wrapper class which allows us to augment the returned {@code value} of a method invocation with some
 * additional "state" information. The instances of this class are used as a cached {@code value} for method
 * invocations in a {@link java.util.concurrent.ConcurrentMap} cache implementation, and their "state" is describing
 * whether the object is currently available, in use or evicted.
 *
 * @see StatementInvocationCacheProvider
 * @see ConnMethodDef
 *
 * @author Simeon Malchev
 * @param <V> the type of the value object held in this ReturnVal
 */
public class ReturnVal<V> {

    /**
     * The 3 different states in which a {@code ReturnVal} object can be while being used as a cached value:
     */
    public static final int AVAILABLE = 0;
    public static final int IN_USE = 1;
    public static final int EVICTED = 2;

    private final V value;
    private final AtomicInteger state;

    public ReturnVal(V value, AtomicInteger state) {
        if (value == null)
            throw new NullPointerException();
        this.value = value;
        this.state = state;
    }

    public V value() {
        return value;
    }

    public AtomicInteger state() {
        return state;
    }
}
