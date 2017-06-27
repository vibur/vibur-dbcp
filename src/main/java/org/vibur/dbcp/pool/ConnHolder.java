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
 * {@link ConnectionFactory} version as well as the "state" needed by the {@link TakenConnection} super-class,
 * i.e., the Connection {@code takenNanoTime} and {@code lastAccessNanoTime}, etc.
 *
 * @author Simeon Malchev
 */
public class ConnHolder extends TakenConnection {

    private final Connection rawConnection; // the underlying raw JDBC Connection
    private final int version; // the version of the ConnectionFactory at the moment of this ConnHolder object creation

    // used when connection validation is enabled via getConnectionIdleLimitInSeconds() >= 0
    private long restoredNanoTime;

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

    long getRestoredNanoTime() {
        return restoredNanoTime;
    }

    void setRestoredNanoTime(long restoredNanoTime) {
        this.restoredNanoTime = restoredNanoTime;
    }
}
