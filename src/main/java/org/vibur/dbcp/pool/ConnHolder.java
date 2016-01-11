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
 * The stateful object which is held in the object pool. It is just a thin wrapper around the
 * raw JDBC {@code Connection} object which allows us to augment it with useful "state" information,
 * such as the Connection last {@code takenTime}, {@code restoredTime}, and similar.
 *
 * @author Simeon Malchev
 */
public class ConnHolder {

    private final Connection value; // the underlying raw JDBC Connection
    private final int version;

    private long takenTime = -1L;
    private long restoredTime;
    private StackTraceElement[] stackTrace = null;

    public ConnHolder(Connection value, int version, long currentTime) {
        if (value == null)
            throw new NullPointerException();
        this.value = value;
        this.version = version;
        this.restoredTime = currentTime;
    }

    public Connection value() {
        return value;
    }

    public int version() {
        return version;
    }

    public long getTakenTime() {
        return takenTime;
    }

    public void setTakenTime(long takenTime) {
        this.takenTime = takenTime;
    }

    public long getRestoredTime() {
        return restoredTime;
    }

    public void setRestoredTime(long restoredTime) {
        this.restoredTime = restoredTime;
    }

    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(StackTraceElement[] stackTrace) {
        this.stackTrace = stackTrace;
    }
}
