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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Arrays;

/**
 * Describes the "parameters" via which a Statement has been created. These are the
 * Connection object, the Method object which has been called on this Connection, and
 * the Method's arguments.

 * <p>Used as a key for Statements caching in a {@link java.util.concurrent.ConcurrentMap}
 * cache implementation.
 *
 * @author Simeon Malchev
 */
public class StatementKey {

    private final Connection connection;
    private final Method method;
    private final Object[] args;

    public StatementKey(Connection connection, Method method, Object[] args) {
        if (connection == null || method == null)
            throw new NullPointerException();
        this.connection = connection;
        this.method = method;
        this.args = args;
    }

    public Connection getConnection() {
        return connection;
    }

    public Method getMethod() {
        return method;
    }

    public Object[] getArgs() {
        return args;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatementKey that = (StatementKey) o;
        return connection.equals(that.connection)
            && method.equals(that.method)
            && Arrays.equals(args, that.args);
    }

    public int hashCode() {
        int result = connection.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + Arrays.hashCode(args);
        return result;
    }
}
