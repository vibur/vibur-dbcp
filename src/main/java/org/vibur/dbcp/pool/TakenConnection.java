/**
 * Copyright 2017 Simeon Malchev
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

import java.sql.Connection;

/**
 * Represents a currently taken proxy Connection, its associated timing data, the thread that has taken it, and
 * the stack trace at the moment when the Connection was taken.
 *
 * @author Simeon Malchev
 */
public interface TakenConnection {

    /**
     * Returns the taken proxy Connection. The application can check whether the Connection is still in taken state
     * via calling the Connection {@code isClosed()} method.
     */
    Connection getProxyConnection();

    /**
     * Returns the nano time when the Connection was taken.
     */
    long getTakenNanoTime();

    /**
     * Returns the nano time when a method was last invoked on this Connection.
     */
    long getLastAccessNanoTime();

    /**
     * Returns the thread that has taken this Connection.
     */
    Thread getThread();

    /**
     * Returns the stack trace at the moment when the connection was taken.
     */
    Throwable getLocation();
}
