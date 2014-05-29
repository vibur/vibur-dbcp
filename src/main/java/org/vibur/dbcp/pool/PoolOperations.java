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
import org.vibur.objectpool.ConcurrentLinkedPool;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.listener.ListenerImpl;
import org.vibur.objectpool.reducer.SamplingPoolReducer;
import org.vibur.objectpool.reducer.ThreadedPoolReducer;

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
public class  PoolOperations {

    private static final Logger logger = LoggerFactory.getLogger(PoolOperations.class);

    private final ViburDBCPConfig config;
    private final Set<String> criticalSQLStates;

    private final ConnectionFactory connectionFactory;
    private final PoolService<ConnHolder> pool;
    private final ThreadedPoolReducer poolReducer;

    /**
     * Instantiates this PoolOperations facade.
     *
     * @param config the ViburDBCPConfig from which will initialize
     * @throws ViburDBCPException if cannot successfully initialize/configure the underlying SQL system
     *          or if cannot create the underlying SQL connections
     */
    public PoolOperations(ViburDBCPConfig config) throws ViburDBCPException {
        if (config == null)
            throw new NullPointerException();

        this.config = config;
        this.criticalSQLStates = new HashSet<String>(Arrays.asList(
            config.getCriticalSQLStates().replaceAll("\\s","").split(",")));

        this.connectionFactory = new ConnectionFactory(config);
        this.pool = new ConcurrentLinkedPool<ConnHolder>(connectionFactory,
            config.getPoolInitialSize(), config.getPoolMaxSize(), config.isPoolFair(),
            config.isPoolEnableConnectionTracking() ?
                new ListenerImpl<ConnHolder>(config.getPoolInitialSize()) : null);

        if (config.getReducerTimeIntervalInSeconds() > 0) {
            this.poolReducer = new PoolReducer(pool,
                config.getReducerTimeIntervalInSeconds(), TimeUnit.SECONDS, config.getReducerSamples());
            this.poolReducer.start();
        } else
            this.poolReducer = null;
    }

    private static class PoolReducer extends SamplingPoolReducer {
        private PoolReducer(PoolService poolService, long timeInterval, TimeUnit unit, int samples) {
            super(poolService, timeInterval, unit, samples);
        }

        protected void afterReduce(int reduction, int reduced, Throwable thrown) {
            if (thrown != null) {
                logger.error(String.format("While trying to reduce the pool by %d elements", reduction), thrown);
                if (!(thrown instanceof ViburDBCPException))
                    terminate();
            } else
                logger.debug("Intended reduction {} actual {}", reduction, reduced);
        }
    }

    public Connection getConnection(long timeout) throws SQLException {
        try {
            return doGetConnection(timeout);
        } catch (ViburDBCPException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException)
                throw (SQLException) cause;
            throw e; // not expected to happen
        }
    }

    private Connection doGetConnection(long timeout) throws SQLException, ViburDBCPException {
        ConnHolder conn = timeout == 0 ?
            pool.take() : pool.tryTake(timeout, TimeUnit.MILLISECONDS);
        if (conn == null)
            throw new SQLException("Couldn't obtain SQL connection.");
        logger.trace("Getting {}", conn.value());
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
                "Critical SQLState %s occurred, destroyed %d connections, current connection version is %d.",
                sqlException.getSQLState(), destroyed, connectionFactory.version()), sqlException);
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

    public void terminate() {
        poolReducer.terminate();
        pool.terminate();
    }

    public PoolService<ConnHolder> getPool() {
        return pool;
    }
}
