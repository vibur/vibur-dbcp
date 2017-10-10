/**
 * Copyright 2014 Simeon Malchev
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
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_INTERRUPTED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_POOL_CLOSED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_TIMEOUT_ERROR;
import static org.vibur.dbcp.proxy.Proxy.newProxyConnection;
import static org.vibur.dbcp.util.JdbcUtils.chainSQLException;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;

/**
 * The facade class through which the {@link ConnectionFactory} and {@link PoolService} functions are accessed.
 * Essentially, these are the operations that allow us to <i>get</i> and <i>restore</i> a JDBC Connection from the pool
 * as well as to process the SQLExceptions that might have occurred on a taken JDBC Connection.
 *
 * @author Simeon Malchev
 */
public class PoolOperations {

    private static final Logger logger = LoggerFactory.getLogger(PoolOperations.class);

    private static final long[] NO_WAIT = {};
    private static final Pattern whitespaces = Pattern.compile("\\s");

    private final ViburDBCPDataSource dataSource;
    private final ViburObjectFactory connectionFactory;
    private final PoolService<ConnHolder> poolService;

    private final Set<String> criticalSQLStates;

    /**
     * Instantiates the PoolOperations facade.
     *
     * @param dataSource the Vibur dataSource on which we will operate
     * @param connectionFactory the Vibur connection factory
     * @param poolService the Vibur pool service
     */
    public PoolOperations(ViburDBCPDataSource dataSource, ViburObjectFactory connectionFactory, PoolService<ConnHolder> poolService) {
        this.dataSource = dataSource;
        this.connectionFactory = connectionFactory;
        this.poolService = poolService;
        this.criticalSQLStates = new HashSet<>(Arrays.asList(
                whitespaces.matcher(dataSource.getCriticalSQLStates()).replaceAll("").split(",")));
    }

    ////////////// getProxyConnection(...) //////////////

    public Connection getProxyConnection(long timeoutMs) throws SQLException {
        ConnHolder conn = getConnHolder(timeoutMs);

        if (logger.isTraceEnabled())
            logger.trace("Taking rawConnection {}", conn.rawConnection());

        Connection proxy = newProxyConnection(conn, this, dataSource);
        if (dataSource.isPoolEnableConnectionTracking())
            conn.setProxyConnection(proxy);
        return proxy;
    }

    private ConnHolder getConnHolder(long timeoutMs) throws SQLException {
        Hook.GetConnection[] onGet = dataSource.getConnHooks().onGet();
        ConnHolder conn = null;
        long[] waitedNanos = NO_WAIT;
        SQLException sqlException = null;

        try {
            if (onGet.length > 0) {
                waitedNanos = new long[1];
                conn = timeoutMs > 0 ? poolService.tryTake(timeoutMs, MILLISECONDS, waitedNanos) : poolService.take(waitedNanos);
            }
            else
                conn = timeoutMs > 0 ? poolService.tryTake(timeoutMs, MILLISECONDS) : poolService.take();

            if (conn == null) // we were *not* able to obtain a connection from the pool
                throw sqlException = createSQLException(onGet.length > 0 ? waitedNanos[0] * 0.000_001 : timeoutMs);

            return conn;

        } catch (ViburDBCPException e) { // can be (indirectly) thrown by the ConnectionFactory.create() methods
            throw sqlException = e.unwrapSQLException();

        } finally {
            Connection rawConnection = conn != null ? conn.rawConnection() : null;
            try {
                for (Hook.GetConnection hook : onGet)
                    hook.on(rawConnection, waitedNanos[0]);

            } catch (SQLException e) {
                // noinspection ThrowFromFinallyBlock - the new exception is just chained to the existing one, if there was such
                throw chainSQLException(sqlException, e);
            }
        }
    }

    private SQLException createSQLException(double timeoutMs) {
        String poolName = getPoolName(dataSource);
        if (poolService.isTerminated())
            return new SQLException(format("Pool %s, the poolService is terminated.", poolName),
                    SQLSTATE_POOL_CLOSED_ERROR);

        boolean isInterrupted = Thread.currentThread().isInterrupted(); // someone else has interrupted us, so we do not clear the flag
        if (!isInterrupted && dataSource.isLogTakenConnectionsOnTimeout() && logger.isWarnEnabled())
            logger.warn(format("Pool %s, couldn't obtain SQL connection within %.3f ms, full list of taken connections begins:\n%s",
                    poolName, timeoutMs, dataSource.getTakenConnectionsStackTraces()));

        int intTimeoutMs = (int) Math.round(timeoutMs);
        return !isInterrupted ?
                new SQLTimeoutException(format("Pool %s, couldn't obtain SQL connection within %.3f ms.",
                        poolName, timeoutMs), SQLSTATE_TIMEOUT_ERROR, intTimeoutMs) :
                new SQLException(format("Pool %s, interrupted while getting SQL connection, waited for %.3f ms.",
                        poolName, timeoutMs), SQLSTATE_INTERRUPTED_ERROR, intTimeoutMs);
    }

    ////////////// restore(...) //////////////

    public void restore(ConnHolder conn, boolean valid, SQLException[] exceptions) {
        if (logger.isTraceEnabled())
            logger.trace("Restoring rawConnection {}", conn.rawConnection());
        boolean reusable = valid && exceptions.length == 0 && conn.version() == connectionFactory.version();
        poolService.restore(conn, reusable);
        processSQLExceptions(conn, exceptions);
    }

    /**
     * Processes SQL exceptions that have occurred on the given JDBC Connection (wrapped in a {@code ConnHolder}).
     *
     * @param conn the given connection
     * @param exceptions the list of SQL exceptions that have occurred on the connection; might be an empty list but not a {@code null}
     */
    private void processSQLExceptions(ConnHolder conn, SQLException[] exceptions) {
        int connVersion = conn.version();
        SQLException criticalException = getCriticalSQLException(exceptions);
        if (criticalException != null && connectionFactory.compareAndSetVersion(connVersion, connVersion + 1)) {
            int destroyed = poolService.drainCreated(); // destroys all connections in the pool
            logger.error("Critical SQLState {} occurred, destroyed {} connections from pool {}, current connection version is {}.",
                    criticalException.getSQLState(), destroyed, getPoolName(dataSource), connectionFactory.version(), criticalException);
        }
    }

    private SQLException getCriticalSQLException(SQLException[] exceptions) {
        for (SQLException exception : exceptions) {
            if (isCriticalSQLException(exception))
                return exception;
        }
        return null;
    }

    private boolean isCriticalSQLException(SQLException exception) {
        if (exception == null)
            return false;
        if (criticalSQLStates.contains(exception.getSQLState()))
            return true;
        return isCriticalSQLException(exception.getNextException());
    }
}
