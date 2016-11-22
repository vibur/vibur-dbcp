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

package org.vibur.dbcp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.ConnectionFactory;
import org.vibur.dbcp.pool.ViburObjectFactory;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_POOL_CLOSED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_TIMEOUT_ERROR;
import static org.vibur.dbcp.proxy.Proxy.newProxyConnection;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.unwrapSQLException;

/**
 * The facade class through which the {@link ConnectionFactory}  and {@link PoolService} functions are accessed.
 * Essentially, these are operations that allows us to get and restore a JDBC connection from the pool,
 * as well as to process the SQLExceptions that might have occurred on a taken JDBC Connection.
 *
 * @author Simeon Malchev
 */
public class PoolOperations {

    private static final Logger logger = LoggerFactory.getLogger(PoolOperations.class);

    private final ViburConfig config;
    private final PoolService<ConnHolder> poolService;
    private final ViburObjectFactory connectionFactory;

    private final Set<String> criticalSQLStates;

    /**
     * Instantiates the PoolOperations facade.
     *
     * @param connectionFactory the object pool connection factory
     * @param poolService the object pool instance
     * @param config the ViburConfig from which we will initialize
     */
    public PoolOperations(ViburObjectFactory connectionFactory, PoolService<ConnHolder> poolService, ViburConfig config) {
        this.config = config;
        this.poolService = poolService;
        this.connectionFactory = connectionFactory;
        this.criticalSQLStates = new HashSet<>(Arrays.asList(config.getCriticalSQLStates()
                .replaceAll("\\s", "").split(",")));
    }

    public Connection getProxyConnection(long timeout) throws SQLException {
        try {
            return doGetProxyConnection(timeout);
        } catch (ViburDBCPException e) {
            return unwrapSQLException(e);
        }
    }

    private Connection doGetProxyConnection(long timeout) throws SQLException, ViburDBCPException {
        ConnHolder conn = timeout == 0 ?
                poolService.take() : poolService.tryTake(timeout, MILLISECONDS);
        if (conn != null) { // we were able to obtain a connection from the pool within the given timeout
            logger.trace("Taking rawConnection {}", conn.value());
            return newProxyConnection(conn, this, config);
        }

        if (poolService.isTerminated())
            throw new SQLException(format("Pool %s, the poolService is terminated.", config.getName()), SQLSTATE_POOL_CLOSED_ERROR);

        if (config.isLogTakenConnectionsOnTimeout() && logger.isWarnEnabled())
            logger.warn("Pool {}, couldn't obtain SQL connection within {}ms, full list of taken connections begins:\n{}",
                    getPoolName(config), timeout, config.takenConnectionsToString());
        throw new SQLTimeoutException(format("Pool %s, couldn't obtain SQL connection within %dms.",
                getPoolName(config), timeout), SQLSTATE_TIMEOUT_ERROR, (int) timeout);
    }

    public void restore(ConnHolder conn, boolean valid, List<Throwable> errors) {
        logger.trace("Restoring rawConnection {}", conn.value());
        boolean reusable = valid && errors.isEmpty() && conn.version() == connectionFactory.version();
        poolService.restore(conn, reusable);
        processSQLExceptions(conn, errors);
    }

    /**
     * Processes SQL exceptions that have occurred on the given JDBC Connection (wrapped in a {@code ConnHolder}).
     *
     * @param conn the given Connection
     * @param errors the list of SQL exceptions that have occurred on the Connection; might be an empty list but not a {@code null}
     */
    private void processSQLExceptions(ConnHolder conn, List<Throwable> errors) {
        int connVersion = conn.version();
        SQLException criticalException = getCriticalSQLException(errors);
        if (criticalException != null && connectionFactory.compareAndSetVersion(connVersion, connVersion + 1)) {
            int destroyed = config.getPool().drainCreated(); // destroys all connections in the pool
            logger.error("Critical SQLState {} occurred, destroyed {} connections from pool {}, current connection version is {}.",
                    criticalException.getSQLState(), destroyed, config.getName(), connectionFactory.version(), criticalException);
        }
    }

    private SQLException getCriticalSQLException(List<Throwable> errors) {
        for (Throwable error : errors) {
            if (error instanceof SQLException) {
                SQLException sqlException = (SQLException) error;
                if (isCriticalSQLException(sqlException))
                    return sqlException;
            }
        }
        return null;
    }

    private boolean isCriticalSQLException(SQLException sqlException) {
        if (sqlException == null)
            return false;
        if (criticalSQLStates.contains(sqlException.getSQLState()))
            return true;
        return isCriticalSQLException(sqlException.getNextException());
    }
}
