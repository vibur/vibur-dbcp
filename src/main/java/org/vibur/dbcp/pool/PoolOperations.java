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
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.proxy.Proxy;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The facade class via which most of the connection pool's and connection factory's functionalities
 * are accessed. Exposes an interface which allows us to:
 *    1. Get and restore a JDBC connection proxy from the pool.
 *    2. Terminate the pool when it is no longer needed.
 *
 * @author Simeon Malchev
 */
public class PoolOperations {

    private static final Logger logger = LoggerFactory.getLogger(PoolOperations.class);

    private final ViburDBCPConfig config;
    private final Set<String> criticalSQLStates;

    private final VersionedObjectFactory<ConnHolder> connectionFactory;
    private final PoolService<ConnHolder> pool;
    private final String name;

    /**
     * Instantiates this PoolOperations facade.
     *
     * @param dataSource the ViburDBCPDataSource from which we will initialize
     */
    public PoolOperations(ViburDBCPDataSource dataSource) throws ViburDBCPException {
        if (dataSource == null)
            throw new NullPointerException();

        this.config = dataSource;
        this.criticalSQLStates = new HashSet<String>(Arrays.asList(
                dataSource.getCriticalSQLStates().replaceAll("\\s", "").split(",")));

        this.connectionFactory = dataSource.getConnectionFactory();
        this.pool = dataSource.getPool();
        this.name = dataSource.getName();
    }

    public Connection getConnection(long timeout) throws SQLException {
        try {
            return doGetConnection(timeout);
        } catch (ViburDBCPException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException)
                throw (SQLException) cause;
            logger.error("Unexpected exception cause", e);
            throw e; // not expected to happen
        }
    }

    private Connection doGetConnection(long timeout) throws SQLException, ViburDBCPException {
        ConnHolder conn = timeout == 0 ?
            pool.take() : pool.tryTake(timeout, TimeUnit.MILLISECONDS);
        if (conn == null)
            throw new SQLException("Couldn't obtain SQL connection from pool " + name);
        logger.trace("Getting {}", conn.value());

        if (config.getConnectionConfigurator() != null)
            config.getConnectionConfigurator().configure(conn.value());
        return Proxy.newConnection(conn, config);
    }

    public boolean restore(ConnHolder conn, boolean aborted, List<Throwable> errors) {
        int connVersion = conn.version();
        boolean valid = !aborted && connVersion == connectionFactory.version() && errors.isEmpty();
        pool.restore(conn, valid);

        SQLException sqlException;
        if ((sqlException = hasCriticalSQLException(errors)) != null
            && connectionFactory.compareAndSetVersion(connVersion, connVersion + 1)) {

            int destroyed = pool.drainCreated(); // destroys all connections in the pool
            logger.error(String.format(
                "Critical SQLState %s occurred, destroyed %d connections from pool %s, current connection version is %d.",
                sqlException.getSQLState(), destroyed, name, connectionFactory.version()), sqlException);
        }
        return valid;
    }

    private SQLException hasCriticalSQLException(List<Throwable> errors) {
        for (Throwable error : errors)
            if (error instanceof SQLException) {
                SQLException sqlException = (SQLException) error;
                if (isCriticalSQLException(sqlException))
                    return sqlException;
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
