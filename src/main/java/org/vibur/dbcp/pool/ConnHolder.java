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
 * JDBC {@code Connection} object which allows us to augment it with useful "state" information, such as
 * the Connection last {@code takenNanoTime} and {@code restoredNanoTime}, and the {@link ConnectionFactory} version.
 *
 * @author Simeon Malchev
 */
public class ConnHolder {

    private final Connection value; // the underlying raw JDBC Connection
    private final int version; // the version of the ConnectionFactory at the moment of this ConnHolder object creation

    // used when there is a CloseConnection hook to measure and emit for how long the connection was held by the app
    private long takenNanoTime = 0;
    private long restoredNanoTime; // used when getConnectionIdleLimitInSeconds() >= 0

    // these 2 fields are used when isPoolEnableConnectionTracking() is allowed
    private Thread thread = null;
    private Throwable location = null;

    ConnHolder(Connection value, int version, long currentNanoTime) {
        assert value != null;
        this.value = value;
        this.version = version;
        this.restoredNanoTime = currentNanoTime;
    }

    public Connection value() {
        return value;
    }

    int version() {
        return version;
    }


    long getTakenNanoTime() {
        return takenNanoTime;
    }

    void setTakenNanoTime(long takenNanoTime) {
        this.takenNanoTime = takenNanoTime;
    }

    long getRestoredNanoTime() {
        return restoredNanoTime;
    }

    void setRestoredNanoTime(long restoredNanoTime) {
        this.restoredNanoTime = restoredNanoTime;
    }


    Thread getThread() {
        return thread;
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Throwable getLocation() {
        return location;
    }

    void setLocation(Throwable location) {
        this.location = location;
    }
}
