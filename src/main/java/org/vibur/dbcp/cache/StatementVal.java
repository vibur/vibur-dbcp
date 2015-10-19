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

import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thin wrapper class which allows us to augment a raw JDBC {@code Statement} object with some additional "state"
 * information. The instances of this class are used as a cached {@code value} (in a {@code ConcurrentMap} cache
 * implementation) for the invocations of {@code Connection.prepareStatement} and {@code Connection.prepareCall}
 * methods, and their "state" is describing whether the object is currently available, in_use, or evicted.
 *
 * @see ConnMethodKey
 *
 * @author Simeon Malchev
 */
public class StatementVal {

    /**
     * The 3 different states in which a StatementVal instance can be, when it is used as a cached value:
     */
    public static final int AVAILABLE = 0;
    public static final int IN_USE = 1;
    public static final int EVICTED = 2;

    private final Statement value; // the underlying raw JDBC Statement
    private final AtomicInteger state; // a null value means that this StatementVal instance is not included in the cache

    public StatementVal(Statement value, AtomicInteger state) {
        if (value == null)
            throw new NullPointerException();
        this.value = value;
        this.state = state;
    }

    public Statement value() {
        return value;
    }

    public AtomicInteger state() {
        return state;
    }
}
