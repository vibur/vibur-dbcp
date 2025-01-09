/**
 * Copyright 2016-2025 Simeon Malchev
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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_CONN_INIT_ERROR;
import static org.vibur.dbcp.util.JdbcUtils.clearWarnings;
import static org.vibur.dbcp.util.JdbcUtils.setDefaultValues;
import static org.vibur.dbcp.util.JdbcUtils.validateOrInitialize;
import static org.vibur.dbcp.util.ViburUtils.formatSql;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * Contains all built-in hooks implementations including hooks for logging of long-lasting getConnection() calls,
 * slow SQL queries, and large ResultSets, as well as hooks for initializing and preparing/clearing of the held in
 * the pool raw connections before and after they're used by the application.
 *
 * @see Hook
 * @see ConnectionFactory
 *
 * @author Simeon Malchev
 */
public abstract class DefaultHook {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHook.class);

    final ViburConfig config;

    private DefaultHook(ViburConfig config) {
        this.config = config;
    }

    abstract boolean isEnabled();

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Connection hooks:

    public static final class InitConnection extends DefaultHook implements Hook.InitConnection {
        public InitConnection(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            if (rawConnection == null) {
                return;
            }

            if (!validateOrInitialize(rawConnection, config.getInitSQL(), config)) {
                throw new SQLException("Couldn't initialize rawConnection " + rawConnection, SQLSTATE_CONN_INIT_ERROR);
            }

            setDefaultValues(rawConnection, config);
        }

        @Override
        boolean isEnabled() {
            return config.getInitSQL() != null ||
                    config.getDefaultAutoCommit() != null || config.getDefaultReadOnly() != null ||
                    config.getDefaultTransactionIsolationIntValue() != null || config.getDefaultCatalog() != null;
        }
    }

    public static final class GetConnectionTiming extends DefaultHook implements Hook.GetConnection {
        public GetConnectionTiming(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(Connection rawConnection, long takenNanos) {
            var takenMillis = takenNanos * 1e-6;
            if (takenMillis < config.getLogConnectionLongerThanMs()) {
                return;
            }

            if (logger.isWarnEnabled()) {
                var log = new StringBuilder(4096)
                        .append(format("Call to getConnection() from pool %s took %f ms, rawConnection = %s",
                                getPoolName(config), takenMillis, rawConnection));
                if (config.isLogStackTraceForLongConnection()) {
                    log.append('\n').append(getStackTraceAsString(config.getLogLineRegex(), new Throwable().getStackTrace()));
                }
                logger.warn(log.toString());
            }
        }

        @Override
        boolean isEnabled() {
            return config.getLogConnectionLongerThanMs() >= 0 && logger.isWarnEnabled();
        }
    }

    public static final class CloseConnection extends DefaultHook implements Hook.CloseConnection {
        public CloseConnection(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(Connection rawConnection, long takenNanos) throws SQLException {
            if (config.isClearSQLWarnings()) {
                clearWarnings(rawConnection);
            }
            if (config.isResetDefaultsAfterUse()) {
                setDefaultValues(rawConnection, config);
            }
        }

        @Override
        boolean isEnabled() {
            return config.isClearSQLWarnings() || config.isResetDefaultsAfterUse();
        }
    }

    public static final class GetConnectionTimeout extends DefaultHook implements Hook.GetConnectionTimeout {
        public GetConnectionTimeout(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(TakenConnection[] takenConnections, long takenNanos) {
            if (logger.isWarnEnabled()) {
                logger.warn("Pool {}, couldn't obtain SQL connection within {} ms, full list of taken connections begins:\n{}",
                        getPoolName(config), format("%.3f", takenNanos * 1e-6),
                        config.getTakenConnectionsFormatter().formatTakenConnections(takenConnections));
            }
        }

        @Override
        boolean isEnabled() {
            return config.isLogTakenConnectionsOnTimeout() && logger.isWarnEnabled();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Invocation hooks:

    public static final class QueryTiming extends DefaultHook implements Hook.StatementExecution {
        public QueryTiming(ViburConfig config) {
            super(config);
        }

        @Override
        public Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
                         StatementProceedingPoint proceed) throws SQLException {

            var startNanoTime = System.nanoTime();
            SQLException sqlException = null;
            try {
                return proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed);

            } catch (SQLException e) {
                throw sqlException = e;
            } finally {
                var takenNanos = System.nanoTime() - startNanoTime;
                logQueryExecution(sqlQuery, sqlQueryParams, takenNanos, sqlException);
            }
        }

        private void logQueryExecution(String sqlQuery, List<Object[]> sqlQueryParams, long takenNanos, SQLException sqlException) {
            var takenMillis = takenNanos * 1e-6;
            var collectQueryStatistics = config.getCollectQueryStatistics();
            var queryStatistics = config.getQueryStatistics();
            var queriesCount = queryStatistics.getQueriesCount();
            var logTime = takenMillis >= config.getLogQueryExecutionLongerThanMs() && logger.isWarnEnabled();
            var logException = sqlException != null && logger.isDebugEnabled();
            var collectStatistics = collectQueryStatistics >= 0 && logger.isInfoEnabled();
            var logStatistics = collectStatistics && collectQueryStatistics > 0 && queriesCount > 0
                    && queriesCount % collectQueryStatistics == 0;
            if (!logTime && !logException && !collectStatistics) {
                return;
            }

            if (logException || logTime || logStatistics) {
                var poolName = getPoolName(config);
                if (logStatistics) {
                    logger.info("SQL query statistics from pool {}, {}", poolName, queryStatistics);
                }

                if (logException || logTime) {
                    var formattedSql = formatSql(sqlQuery, sqlQueryParams);

                    if (logException) {
                        logger.debug("SQL query execution from pool {}:\n{}\n-- threw:", poolName, formattedSql, sqlException);
                    }

                    if (logTime) {
                        var message = new StringBuilder(4096).append(
                                format("SQL query execution from pool %s took %f ms:\n%s", poolName, takenMillis, formattedSql));
                        if (config.isLogStackTraceForLongQueryExecution()) {
                            message.append('\n').append(getStackTraceAsString(config.getLogLineRegex(), new Throwable().getStackTrace()));
                        }
                        logger.warn(message.toString());
                    }
                }
            }

            if (collectStatistics) {
                queryStatistics.accept(takenNanos);
                if (sqlException != null) {
                    queryStatistics.incrementExceptions();
                }
            }
        }

        @Override
        boolean isEnabled() {
            return (config.getLogQueryExecutionLongerThanMs() >= 0 && logger.isWarnEnabled())
                    || (config.getCollectQueryStatistics() >= 0 && logger.isInfoEnabled());
        }
    }

    public static final class ResultSetSize extends DefaultHook implements Hook.ResultSetRetrieval {
        public ResultSetSize(ViburConfig config) {
            super(config);
        }

        @Override
        public void on(String sqlQuery, List<Object[]> sqlQueryParams, long resultSetSize, long resultSetNanoTime) {
            if (config.getLogLargeResultSet() > resultSetSize) {
                return;
            }

            if (logger.isWarnEnabled()) {
                var message = new StringBuilder(4096).append(
                        format("SQL query execution from pool %s retrieved a ResultSet with size %d, total retrieval and processing time %f ms:\n%s",
                                getPoolName(config), resultSetSize, resultSetNanoTime * 1e-6, formatSql(sqlQuery, sqlQueryParams)));
                if (config.isLogStackTraceForLargeResultSet()) {
                    message.append('\n').append(getStackTraceAsString(config.getLogLineRegex(), new Throwable().getStackTrace()));
                }
                logger.warn(message.toString());
            }
        }

        @Override
        boolean isEnabled() {
            return config.getLogLargeResultSet() >= 0 && logger.isWarnEnabled();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Hooks utils:

    static final class Util {

        private Util() { }

        static <T extends Hook> T[] addHook(T[] hooks, T hook) {
            requireNonNull(hook);
            if (hook instanceof DefaultHook && !((DefaultHook) hook).isEnabled()) {
                return hooks;
            }

            for (Hook h : hooks) {
                if (h.equals(hook)) { // don't add the same hook twice
                    return hooks;
                }
            }

            var length = hooks.length;
            hooks = Arrays.copyOf(hooks, length + 1); // i.e., copy-on-write
            hooks[length] = hook;
            return hooks;
        }
    }
}
