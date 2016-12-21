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

package org.vibur.dbcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.pool.Hook;

import java.sql.Connection;
import java.util.List;

import static java.lang.String.format;
import static org.vibur.dbcp.util.QueryUtils.formatSql;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * Vibur logging implementation of long lasting getConnection() calls, slow SQL queries, and large ResultSets.
 *
 * @author Simeon Malchev
 */
// todo become class...
interface ViburLogger {

    // todo ... fix me
    static final Logger logger = LoggerFactory.getLogger(ViburLogger.class);

    static class QueryTiming extends HookConfig implements Hook.StatementExecution {
        QueryTiming(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(String sqlQuery, List<Object[]> queryParams, long takenNanos) {
            double takenMillis = takenNanos / 1000000.0;
            if (takenMillis < config.getLogQueryExecutionLongerThanMs())
                return;

            StringBuilder message = new StringBuilder(4096).append(
                    format("SQL query execution from pool %s took %f ms:\n%s",
                            poolName, takenMillis, formatSql(sqlQuery, queryParams)));
            if (config.isLogStackTraceForLongQueryExecution())
                message.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(message.toString());
        }
    }

    static class ResultSetSize extends HookConfig implements Hook.ResultSetRetrieval {
        ResultSetSize(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(String sqlQuery, List<Object[]> queryParams, long resultSetSize) {
            if (config.getLogLargeResultSet() > resultSetSize)
                return;

            StringBuilder message = new StringBuilder(4096).append(
                    format("SQL query execution from pool %s retrieved a ResultSet with size %d:\n%s",
                            poolName, resultSetSize, formatSql(sqlQuery, queryParams)));
            if (config.isLogStackTraceForLargeResultSet())
                message.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(message.toString());
        }
    }

    // todo ...
    static abstract class HookConfig {
        final ViburConfig config;
        final String poolName;

        HookConfig(ViburConfig config) {
            this.config = config;
            this.poolName = getPoolName(config);
        }
    }

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
}
