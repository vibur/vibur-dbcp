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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thin wrapper class which allows us to augment the returned value of a Method invocation with some additional
 * "state" information, in this case describing whether the returned object is currently in use or not.
 *
 * <p>Used as a cached {@code value} for method invocations in a {@link java.util.concurrent.ConcurrentMap}
 * cache implementation.
 *
 * @see StatementInvocationCacheProvider
 *
 * @author Simeon Malchev
 */
public class ReturnVal<V> {
    private final V value;
    private final AtomicBoolean inUse;

    public ReturnVal(V value, AtomicBoolean inUse) {
        if (value == null)
            throw new NullPointerException();
        this.value = value;
        this.inUse = inUse;
    }

    public V value() {
        return value;
    }

    public AtomicBoolean inUse() {
        return inUse;
    }
}
