/**
 * Copyright 2014 Daniel Caldeweyher
 * Copyright 2013 Simeon Malchev
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
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburDBCPConfig.SQLSTATE_CONN_INIT_ERROR;
import static org.vibur.dbcp.util.JdbcUtils.*;

/**
 * The object factory which controls the lifecycle of the underlying JDBC Connections: creates them,
 * validates them if needed, and destroys them. Used by {@link org.vibur.dbcp.ViburDBCPDataSource}.
 *
 * <p>This {@code ConnectionFactory} is a {@code VersionedObject} which creates versioned JDBC Connection
 * wrappers {@code ConnHolder(s)}. The version of each {@code ConnHolder} created by the factory is the same
 * as the version of the factory at the moment of the object creation.
 *
 * @author Simeon Malchev
 * @author Daniel Caldeweyher
 */
public class ConnectionFactory implements ViburObjectFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);

    private final ViburDBCPConfig config;
    private final Set<String> criticalSQLStates;

    private final AtomicInteger version = new AtomicInteger(1);

    /**
     * Instantiates this object factory.
     *
     * @param config the ViburDBCPConfig from which will initialize
     * @throws ViburDBCPException if cannot successfully initialize/configure the underlying SQL system
     */
    @SuppressWarnings("unchecked")
    public ConnectionFactory(ViburDBCPConfig config) throws ViburDBCPException {
        this.config = requireNonNull(config);
        this.criticalSQLStates = new HashSet<>(Arrays.asList(
                config.getCriticalSQLStates().replaceAll("\\s", "").split(",")));

        initLoginTimeout(config);
        initJdbcDriver(config);
    }

    /**
     * {@inheritDoc}
     *
     * @throws org.vibur.dbcp.ViburDBCPException if cannot create the underlying JDBC Connection.
     */
    @Override
    public ConnHolder create() throws ViburDBCPException {
        return create(config.getUsername(), config.getPassword());
    }

    public ConnHolder create(String userName, String password) throws ViburDBCPException {
        int attempt = 0;
        Connection rawConnection = null;
        while (rawConnection == null) {
            try {
                rawConnection = createConnection(config, userName, password);
            } catch (SQLException e) {
                logger.debug("Couldn't create a java.sql.Connection, attempt {}", attempt, e);
                if (attempt++ >= config.getAcquireRetryAttempts())
                    throw new ViburDBCPException(e);
                try {
                    MILLISECONDS.sleep(config.getAcquireRetryDelayInMs());
                } catch (InterruptedException ignore) {
                }
            }
        }

        try {
            if (config.getInitConnectionHook() != null)
                config.getInitConnectionHook().on(rawConnection);
            ensureConnectionInitialized(rawConnection);
            setDefaultValues(config, rawConnection);
        } catch (SQLException e) {
            quietClose(rawConnection);
            throw new ViburDBCPException(e);
        }
        logger.debug("Created {}", rawConnection);
        return new ConnHolder(rawConnection, version(), System.currentTimeMillis());
    }

    private void ensureConnectionInitialized(Connection rawConnection) throws SQLException {
        if (!validateConnection(config, rawConnection, config.getInitSQL()))
            throw new SQLException("Couldn't initialize " + rawConnection, SQLSTATE_CONN_INIT_ERROR);
    }

    @Override
    public boolean readyToTake(ConnHolder conn) {
        if (conn.version() != version())
            return false;

        Connection rawConnection = conn.value();
        try {
            int idleLimit = config.getConnectionIdleLimitInSeconds();
            if (idleLimit >= 0) {
                int idle = (int) MILLISECONDS.toSeconds(System.currentTimeMillis() - conn.getRestoredTime());
                if (idle >= idleLimit && !validateConnection(config, rawConnection, config.getTestConnectionQuery()))
                    return false;
            }
            if (config.getConnectionHook() != null)
                config.getConnectionHook().on(rawConnection);

            if (config.isPoolEnableConnectionTracking()) {
                conn.setTakenTime(System.currentTimeMillis());
                conn.setStackTrace(new Throwable().getStackTrace());
            }
            return true;
        } catch (SQLException e) {
            logger.debug("Couldn't validate {}", rawConnection, e);
            processSQLExceptions(conn, Collections.<Throwable>singletonList(e));
            return false;
        }
    }

    @Override
    public boolean readyToRestore(ConnHolder conn) {
        Connection rawConnection = conn.value();
        try {
            if (config.getCloseConnectionHook() != null)
                config.getCloseConnectionHook().on(rawConnection);
            if (config.isClearSQLWarnings())
                clearWarnings(rawConnection);
            if (config.isResetDefaultsAfterUse())
                setDefaultValues(config, rawConnection);

            conn.setRestoredTime(System.currentTimeMillis());
            return true;
        } catch (SQLException e) {
            logger.debug("Couldn't reset {}", rawConnection, e);
            processSQLExceptions(conn, Collections.<Throwable>singletonList(e));
            return false;
        }
    }

    @Override
    public void destroy(ConnHolder conn) {
        Connection rawConnection = conn.value();
        logger.debug("Destroying {}", rawConnection);
        closeStatements(rawConnection);
        quietClose(rawConnection);
    }

    private void closeStatements(Connection rawConnection) {
        StatementCache statementCache = config.getStatementCache();
        if (statementCache != null)
            statementCache.removeAll(rawConnection);
    }

    @Override
    public int version() {
        return version.get();
    }

    @Override
    public boolean compareAndSetVersion(int expect, int update) {
        return version.compareAndSet(expect, update);
    }

    @Override
    public void processSQLExceptions(ConnHolder conn, List<Throwable> errors) {
        int connVersion = conn.version();
        SQLException criticalException = getCriticalSQLException(errors);
        if (criticalException != null && compareAndSetVersion(connVersion, connVersion + 1)) {
            int destroyed = config.getPool().drainCreated(); // destroys all connections in the pool
            logger.error("Critical SQLState {} occurred, destroyed {} connections from pool {}, current connection version is {}.",
                    criticalException.getSQLState(), destroyed, config.getName(), version(), criticalException);
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
