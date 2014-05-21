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

import org.vibur.objectpool.util.IdBasedHolder;

import java.sql.Connection;

/**
 * Represents the state object which we hold in the object pool. It is just a thin wrapper class
 * which allows us to augment the Connection object with useful additional "state" information
 * like the {@code lastTimeUsed} and similar.
 *
 * @author Simeon Malchev
 */
public class ConnHolder extends IdBasedHolder<Connection> {

    private final int version;
    private final StackTraceElement[] stackTrace;

    private long lastTimeUsed;
    private final long createdTime;

    public ConnHolder(long id, Connection connection, int version, long currentTime) {
        super(id, connection);
        this.version = version;
        this.lastTimeUsed = currentTime;
        this.createdTime = currentTime;
        this.stackTrace = new Throwable().getStackTrace(); // todo...
    }

    public int getVersion() {
        return version;
    }

    public long getLastTimeUsed() {
        return lastTimeUsed;
    }

    public void setLastTimeUsed(long lastTimeUsed) {
        this.lastTimeUsed = lastTimeUsed;
    }

    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    public long getCreatedTime() {
        return createdTime;
    }
}
