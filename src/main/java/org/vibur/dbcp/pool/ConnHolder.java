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

package org.vibur.dbcp.pool;

import java.sql.Connection;

/**
 * The stateful versioned object which is held in the object pool. It is just a thin wrapper around the raw
 * JDBC {@code Connection} object which allows us to augment it with useful "state" information such as the
 * {@link ConnectionFactory} version as well as the "state" needed by the {@link TakenConnection} interface,
 * i.e., the Connection {@code takenNanoTime} and {@code lastAccessNanoTime}, etc.
 *
 * @author Simeon Malchev
 */
public class ConnHolder implements TakenConnection {

    private final Connection rawConnection; // the underlying raw JDBC Connection
    private final int version; // the version of the ConnectionFactory at the moment of this ConnHolder object creation

    // used when poolEnableConnectionTracking is allowed or if there are GetConnection or CloseConnection hooks registered
    private long takenNanoTime = 0;
    // the last nano time when a method was called on the proxyConnection, used when poolEnableConnectionTracking is allowed
    private long lastAccessNanoTime = 0;
    // used when connection validation is enabled via getConnectionIdleLimitInSeconds() >= 0
    private long restoredNanoTime;
    // the proxy Connection encompassing the rawConnection, used when poolEnableConnectionTracking is allowed
    private Connection proxyConnection = null;

    // these 2 fields are used when poolEnableConnectionTracking is allowed
    private Thread thread = null;
    private Throwable location = null;

    ConnHolder(Connection rawConnection, int version, long currentNanoTime) {
        assert rawConnection != null;
        this.rawConnection = rawConnection;
        this.version = version;
        this.restoredNanoTime = currentNanoTime;
    }

    public Connection rawConnection() {
        return rawConnection;
    }

    int version() {
        return version;
    }

    @Override
    public long getTakenNanoTime() {
        return takenNanoTime;
    }

    void setTakenNanoTime(long takenNanoTime) {
        this.takenNanoTime = takenNanoTime;
    }

    @Override
    public long getLastAccessNanoTime() {
        return lastAccessNanoTime;
    }

    public void setLastAccessNanoTime(long lastAccessNanoTime) {
        this.lastAccessNanoTime = lastAccessNanoTime;
    }

    long getRestoredNanoTime() {
        return restoredNanoTime;
    }

    void setRestoredNanoTime(long restoredNanoTime) {
        this.restoredNanoTime = restoredNanoTime;
    }

    @Override
    public Connection getProxyConnection() {
        return proxyConnection;
    }

    public void setProxyConnection(Connection proxyConnection) {
        this.proxyConnection = proxyConnection;
    }

    @Override
    public Thread getThread() {
        return thread;
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    @Override
    public Throwable getLocation() {
        return location;
    }

    void setLocation(Throwable location) {
        this.location = location;
    }
}
