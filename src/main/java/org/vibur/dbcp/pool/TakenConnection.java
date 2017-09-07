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

import static java.lang.Integer.toHexString;

/**
 * Represents a currently taken proxy Connection, its associated timing data, the thread that has taken it, and
 * the stack trace at the moment when the Connection was taken. Most of the fields of this class are used only if
 * {@link org.vibur.dbcp.ViburConfig#poolEnableConnectionTracking poolEnableConnectionTracking} is allowed.
 *
 * @author Simeon Malchev
 */
public abstract class TakenConnection {

    TakenConnection() { }

    // the proxy Connection, used when poolEnableConnectionTracking is allowed
    private Connection proxyConnection = null;
    // used when poolEnableConnectionTracking is allowed or if there are GetConnection or CloseConnection hooks registered
    private long takenNanoTime = 0;
    // the last nano time when a method was called on the proxyConnection, used when poolEnableConnectionTracking is allowed
    private long lastAccessNanoTime = 0;

    // these 2 fields are used when poolEnableConnectionTracking is allowed
    private Thread thread = null;
    private Throwable location = null;

    /**
     * Returns the taken proxy Connection. The application can check whether the Connection is still in taken state
     * via calling the Connection {@code isClosed()} method.
     */
    public Connection getProxyConnection() {
        return proxyConnection;
    }

    void setProxyConnection(Connection proxyConnection) {
        this.proxyConnection = proxyConnection;
    }

    /**
     * Returns the nano time when the Connection was taken.
     */
    public long getTakenNanoTime() {
        return takenNanoTime;
    }

    void setTakenNanoTime(long takenNanoTime) {
        this.takenNanoTime = takenNanoTime;
    }

    /**
     * Returns the nano time when a method was last invoked on this Connection. Only "restricted" methods
     * invocations are updating the {@code lastAccessNanoTime}, i.e., methods such as {@code close()},
     * {@code isClosed()} do not update the {@code lastAccessNanoTime}. See
     * {@link org.vibur.dbcp.proxy.ConnectionInvocationHandler#restrictedInvoke restrictedInvoke()} for more details.
     * A value of {@code 0} indicates that a restricted method was never called on this Connection proxy.
     */
    public long getLastAccessNanoTime() {
        return lastAccessNanoTime;
    }

    void setLastAccessNanoTime(long lastAccessNanoTime) {
        this.lastAccessNanoTime = lastAccessNanoTime;
    }

    /**
     * Returns the thread that has taken this Connection.
     */
    public Thread getThread() {
        return thread;
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }


    /**
     * Returns the stack trace at the moment when the connection was taken.
     */
    public Throwable getLocation() {
        return location;
    }

    void setLocation(Throwable location) {
        this.location = location;
    }

    @Override
    public String toString() {
        long currentNanoTime = System.nanoTime();
        return "TakenConnection@" + toHexString(hashCode()) + '[' + proxyConnection +
                ", takenNanoTime=" + nanosToMillis(takenNanoTime, currentNanoTime) +
                " ms, " + (lastAccessNanoTime == 0 ? "has not been accessed" :
                    "lastAccessNanoTime=" + nanosToMillis(lastAccessNanoTime, currentNanoTime) + " ms") +
                ", thread=" + thread +
                (thread != null ? ", state=" + thread.getState() : "") +
                ']';
    }

    private static double nanosToMillis(long pastNanoTime, long currentNanoTime) {
        return (currentNanoTime - pastNanoTime) * 0.000_001;
    }
}
