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

package org.vibur.dbcp.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_CONN_INIT_ERROR;
import static org.vibur.dbcp.util.JdbcUtils.clearWarnings;
import static org.vibur.dbcp.util.JdbcUtils.setDefaultValues;
import static org.vibur.dbcp.util.JdbcUtils.validateConnection;
import static org.vibur.dbcp.util.QueryUtils.formatSql;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * Contains all built-in hooks implementations including hooks for logging of long lasting getConnection() calls,
 * slow SQL queries, and large ResultSets, as well as hooks for initializing and preparing/clearing of the held in
 * the pool raw connections before and after they're used by the application.
 *
 * @see Hook
 * @see ConnectionFactory
 *
 * @author Simeon Malchev
 */
public class ViburHook {

    private static final Logger logger = LoggerFactory.getLogger(ViburHook.class);

    private final ViburConfig config;

    private ViburHook(ViburConfig config) {
        this.config = config;
        initHooks();
    }

    public static void initializeHooks(ViburConfig config) {
        new ViburHook(config);
    }

    private void initHooks() {
        config.getConnHooks().addOnInit(new ViburHook.InitConnection());
        config.getConnHooks().addOnClose(new ViburHook.CloseConnection());

        if (config.getLogConnectionLongerThanMs() >= 0)
            config.getConnHooks().addOnGet(new ViburHook.GetConnectionTiming());
        if (config.getLogQueryExecutionLongerThanMs() >= 0)
            config.getInvocationHooks().addOnStatementExecution(new ViburHook.QueryTiming());
        if (config.getLogLargeResultSet() >= 0)
            config.getInvocationHooks().addOnResultSetRetrieval(new ViburHook.ResultSetSize());
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    private class InitConnection implements Hook.InitConnection {
        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            if (!validateConnection(rawConnection, config.getInitSQL(), config))
                throw new SQLException("Couldn't initialize rawConnection " + rawConnection, SQLSTATE_CONN_INIT_ERROR);

            setDefaultValues(rawConnection, config);
        }
    }

    private class CloseConnection implements Hook.CloseConnection {
        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            if (config.isClearSQLWarnings())
                clearWarnings(rawConnection);
            if (config.isResetDefaultsAfterUse())
                setDefaultValues(rawConnection, config);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    private class GetConnectionTiming implements Hook.GetConnection {
        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            double takenMillis = takenNanos / 1000000.0;
            if (takenMillis < config.getLogConnectionLongerThanMs())
                return;

            StringBuilder log = new StringBuilder(4096)
                    .append(format("Call to getConnection() from pool %s took %f ms, connProxy = %s",
                            getPoolName(config), takenMillis, rawConnection));
            if (config.isLogStackTraceForLongConnection() )
                log.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(log.toString());
        }
    }

    private class QueryTiming implements Hook.StatementExecution {
        @Override
        public void on(String sqlQuery, List<Object[]> queryParams, long takenNanos) {
            double takenMillis = takenNanos / 1000000.0;
            if (takenMillis < config.getLogQueryExecutionLongerThanMs())
                return;

            StringBuilder message = new StringBuilder(4096).append(
                    format("SQL query execution from pool %s took %f ms:\n%s",
                            getPoolName(config), takenMillis, formatSql(sqlQuery, queryParams)));
            if (config.isLogStackTraceForLongQueryExecution())
                message.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(message.toString());
        }
    }

    private class ResultSetSize implements Hook.ResultSetRetrieval {
        @Override
        public void on(String sqlQuery, List<Object[]> queryParams, long resultSetSize) {
            if (config.getLogLargeResultSet() > resultSetSize)
                return;

            StringBuilder message = new StringBuilder(4096).append(
                    format("SQL query execution from pool %s retrieved a ResultSet with size %d:\n%s",
                            getPoolName(config), resultSetSize, formatSql(sqlQuery, queryParams)));
            if (config.isLogStackTraceForLargeResultSet())
                message.append('\n').append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(message.toString());
        }
    }
}
