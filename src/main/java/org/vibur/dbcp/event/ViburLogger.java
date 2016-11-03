/**
 * Copyright 2015 Simeon Malchev
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

package org.vibur.dbcp.event;

import org.vibur.dbcp.ViburConfig;

import java.sql.Connection;
import java.util.List;

/**
 * Vibur logging of long lasting getConnection() calls, slow SQL queries, and large ResultSets - operations definitions.
 *
 * @see BaseViburLogger
 *
 * @author Simeon Malchev
 */
public interface ViburLogger {

    /**
     * This method will be called by Vibur DBCP when a call to getConnection() has taken longer than what is
     * specified by {@link ViburConfig#logConnectionLongerThanMs}.
     *
     * @param poolName the pool name
     * @param connProxy the current connection proxy - can be {@code null} which means that the
     *                  {@code getConnection()} call was not able to retrieve a connection from the pool in
     *                  the specified time limit
     * @param timeTaken the time taken by the {@code getConnection()} method to complete in milliseconds
     * @param stackTrace the stack trace of the {@code getConnection()} method call (or null), depending on
     *                   {@link ViburConfig#logStackTraceForLongConnection}
     */
    void logGetConnection(String poolName, Connection connProxy, long timeTaken,
                          StackTraceElement[] stackTrace);

    /**
     * This method will be called by Vibur DBCP when an SQL query execution has taken longer than what is
     * specified by {@link ViburConfig#logQueryExecutionLongerThanMs}.
     *
     * @param poolName the pool name
     * @param sqlQuery the executed SQL query
     * @param queryParams the executed SQL query params
     * @param timeTaken the time taken by the executed SQL query to complete in milliseconds; also see the comments for
     *                  {@link ViburConfig#logQueryExecutionLongerThanMs}
     * @param stackTrace the stack trace of the method call (or null) via which the executed SQL query was initiated,
     *                   depending on {@link ViburConfig#logStackTraceForLongQueryExecution}
     */
    void logQuery(String poolName, String sqlQuery, List<Object[]> queryParams, long timeTaken,
                  StackTraceElement[] stackTrace);

    /**
     * This method will be called by Vibur DBCP when an SQL query has retrieved a ResultSet larger than what is
     * specified by {@link ViburConfig#logLargeResultSet}.
     *
     * @param poolName the pool name
     * @param sqlQuery the executed SQL query
     * @param queryParams the executed SQL query params
     * @param resultSetSize the retrieved ResultSet size
     * @param stackTrace the stack trace of the method call (or null) via which the SQL query that has
     *                   retrieved the large ResultSet was initiated, depending on
     *                   {@link ViburConfig#logStackTraceForLargeResultSet}
     */
    void logResultSetSize(String poolName, String sqlQuery, List<Object[]> queryParams, long resultSetSize,
                          StackTraceElement[] stackTrace);
}
