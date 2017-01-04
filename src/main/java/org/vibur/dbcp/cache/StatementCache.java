/**
 * Copyright 2016 Simeon Malchev
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

import java.sql.Connection;
import java.sql.Statement;

/**
 * Defines the operations needed for the JDBC Statement caching.
 *
 * @author Simeon Malchev
 */
public interface StatementCache {

    /**
     * Returns <i>a possibly</i> cached StatementHolder object for the given connection statement method.
     *
     * @param statementMethod the statement method
     * @return a retrieved from the cache or newly created StatementHolder object wrapping the raw JDBC Statement object
     * @throws Throwable if the invoked underlying "prepare..." method throws an exception
     */
    StatementHolder take(StatementMethod statementMethod) throws Throwable;

    /**
     * Returns (i.e. marks as available) the given {@code StatementHolder} back to the cache.
     *
     * @param statement the given {@code StatementHolder}
     * @param clearWarnings if {@code true} will execute {@link Statement#clearWarnings} on the underlying raw Statement
     */
    void restore(StatementHolder statement, boolean clearWarnings);

    /**
     * Removes an entry from the cache (if such) for the given {@code rawStatement}. Does <b>not</b> close
     * the removed statement.
     *
     * @param rawStatement the statement to be removed
     * @return true if success, false otherwise
     */
    boolean remove(Statement rawStatement);

    /**
     * Removes all entries from the cache (if any) for the given {@code rawConnection}. Closes
     * all removed statements.
     *
     * @param rawConnection the connection for which the entries to be removed
     * @return the number of removed entries
     */
    int removeAll(Connection rawConnection);

    /**
     * Closes this StatementCache and removes all entries from it.
     */
    void close();

    /**
     * Returns {@code true} if this {@code StatementCache} is closed; {@code false} otherwise.
     */
    boolean isClosed();
}
