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
import java.sql.SQLException;
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
class ViburLogger {

    private static final Logger logger = LoggerFactory.getLogger(ViburLogger.class);

    static class GetConnectionTiming extends HookConfig implements Hook.GetConnection {
        GetConnectionTiming(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            double takenMillis = takenNanos / 1000000.0;
            if (takenMillis < config.getLogConnectionLongerThanMs())
                return;

            StringBuilder log = new StringBuilder(4096)
                    .append(format("Call to getConnection() from pool %s took %f ms, connProxy = %s",
                            poolName, takenMillis, rawConnection));
            if (config.isLogStackTraceForLongConnection() )
                log.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(log.toString());
        }
    }

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

    private static abstract class HookConfig {
        final ViburConfig config;
        final String poolName;

        HookConfig(ViburConfig config) {
            this.config = config;
            this.poolName = getPoolName(config);
        }
    }
}
