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
import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.ViburDBCPException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_CONN_INIT_ERROR;
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

    private final ViburConfig config;
    private final AtomicInteger version = new AtomicInteger(1);

    /**
     * Instantiates this object factory.
     *
     * @param config the ViburConfig from which will initialize
     * @throws ViburDBCPException if cannot successfully initialize/configure the underlying SQL system
     */
    public ConnectionFactory(ViburConfig config) throws ViburDBCPException {
        this.config = config;
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
                rawConnection = createConnection(userName, password, config);
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
            ensureInitialized(rawConnection);
            setDefaultValues(rawConnection, config);
        } catch (SQLException e) {
            quietClose(rawConnection);
            throw new ViburDBCPException(e);
        }
        logger.debug("Created {}", rawConnection);
        return new ConnHolder(rawConnection, version(), System.currentTimeMillis());
    }

    private void ensureInitialized(Connection rawConnection) throws SQLException {
        if (!validateConnection(rawConnection, config.getInitSQL(), config))
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
                if (idle >= idleLimit && !validateConnection(rawConnection, config.getTestConnectionQuery(), config))
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
                setDefaultValues(rawConnection, config);

            conn.setRestoredTime(System.currentTimeMillis());
            return true;
        } catch (SQLException e) {
            logger.debug("Couldn't reset {}", rawConnection, e);
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
        if (config.getStatementCache() != null)
            config.getStatementCache().removeAll(rawConnection);
    }

    @Override
    public int version() {
        return version.get();
    }

    @Override
    public boolean compareAndSetVersion(int expect, int update) {
        return version.compareAndSet(expect, update);
    }
}
