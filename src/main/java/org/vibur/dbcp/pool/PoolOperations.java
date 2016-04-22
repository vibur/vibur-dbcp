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
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.proxy.Proxy;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburDBCPConfig.SQLSTATE_POOL_CLOSED_ERROR;
import static org.vibur.dbcp.ViburDBCPConfig.SQLSTATE_TIMEOUT_ERROR;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.unwrapSQLException;

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
    private final PoolService<ConnHolder> pool;
    private final ViburObjectFactory viburObjectFactory;

    /**
     * Instantiates this PoolOperations facade.
     *
     * @param config the ViburDBCPConfig from which we will initialize
     */
    @SuppressWarnings("unchecked")
    public PoolOperations(ViburDBCPConfig config, ViburObjectFactory viburObjectFactory) {
        this.config = requireNonNull(config);
        this.pool = (PoolService<ConnHolder>) config.getPool();
        this.viburObjectFactory = requireNonNull(viburObjectFactory);
    }

    public Connection getConnection(long timeout) throws SQLException {
        try {
            return doGetConnection(timeout);
        } catch (ViburDBCPException e) {
            return unwrapSQLException(e);
        }
    }

    private Connection doGetConnection(long timeout) throws SQLException, ViburDBCPException {
        ConnHolder conn = timeout == 0 ?
            pool.take() : pool.tryTake(timeout, MILLISECONDS);
        if (conn == null) {
            if (pool.isTerminated())
                throw new SQLException(format("Pool %s is terminated.", config.getName()), SQLSTATE_POOL_CLOSED_ERROR);
            else
                throw new SQLException(format("Couldn't obtain SQL connection from pool %s within %dms.",
                        getPoolName(config), timeout), SQLSTATE_TIMEOUT_ERROR, (int) timeout);
        }
        logger.trace("Getting {}", conn.value());
        return Proxy.newConnection(conn, config);
    }

    public boolean restore(ConnHolder conn, boolean aborted, List<Throwable> errors) {
        boolean valid = !aborted && errors.isEmpty() && conn.version() == viburObjectFactory.version();
        pool.restore(conn, valid);
        viburObjectFactory.processSQLExceptions(conn, errors);
        return valid;
    }
}
