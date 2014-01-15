/**
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

package org.vibur.dbcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.listener.DestroyListener;
import org.vibur.objectpool.PoolObjectFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * The object factory which controls the lifecycle of the underlying JDBC Connections: creates them,
 * validates them if needed, and destroys them. Used by {@link ViburDBCPDataSource}.
 *
 * @author Simeon Malchev
 */
public class ConnectionObjectFactory implements PoolObjectFactory<ConnState> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionObjectFactory.class);

    private final ViburDBCPConfig config;
    private final DestroyListener destroyListener;

    public ConnectionObjectFactory(ViburDBCPConfig config, DestroyListener destroyListener) {
        if (config == null || destroyListener == null)
            throw new NullPointerException();
        this.config = config;
        this.destroyListener = destroyListener;

        initLoginTimeout(config);
    }

    private void initLoginTimeout(ViburDBCPConfig config) {
        int loginTimeout = (int) config.getConnectionTimeoutInMs() / 1000;
        if (config.getExternalDataSource() == null)
            DriverManager.setLoginTimeout(loginTimeout);
        else
            try {
                config.getExternalDataSource().setLoginTimeout(loginTimeout);
            } catch (SQLException e) {
                logger.warn("Couldn't set the login timeout to the configured external DataSource", e);
            }
    }

    /**
     * {@inheritDoc}
     *
     * @throws ViburDBCPException if cannot create the underlying JDBC Connection.
     */
    public ConnState create() throws ViburDBCPException {
        int attempt = 0;
        Connection connection = null;
        while (connection == null) {
            try {
                connection = doCreate();
            } catch (SQLException e) {
                logger.debug("Couldn't create a java.sql.Connection, attempt " + attempt, e);
                if (attempt++ >= config.getAcquireRetryAttempts())
                    throw new ViburDBCPException(e);
                try {
                    TimeUnit.MILLISECONDS.sleep(config.getAcquireRetryDelayInMs());
                } catch (InterruptedException ignore) {
                }
            }
        }

        setDefaultValues(connection);
        logger.trace("Created {}", connection);
        return new ConnState(connection, System.currentTimeMillis());
    }

    private Connection doCreate() throws SQLException {
        Connection connection;
        DataSource externalDataSource = config.getExternalDataSource();
        if (externalDataSource == null) {
            if (config.getUsername() != null)
                connection = DriverManager.getConnection(config.getJdbcUrl(),
                    config.getUsername(), config.getPassword());
            else
                connection = DriverManager.getConnection(config.getJdbcUrl());
        } else {
            if (config.getUsername() != null)
                connection = externalDataSource.getConnection(config.getUsername(), config.getPassword());
            else
                connection = externalDataSource.getConnection();
        }
        return connection;
    }

    private void setDefaultValues(Connection connection) throws ViburDBCPException {
        try {
            if (config.getDefaultAutoCommit() != null)
                connection.setAutoCommit(config.getDefaultAutoCommit());
            if (config.getDefaultReadOnly() != null)
                connection.setReadOnly(config.getDefaultReadOnly());
            if (config.getDefaultTransactionIsolationValue() != null)
                connection.setTransactionIsolation(config.getDefaultTransactionIsolationValue());
            if (config.getDefaultCatalog() != null)
                connection.setCatalog(config.getDefaultCatalog());
        } catch (SQLException e) {
            throw new ViburDBCPException(e);
        }
    }

    /** {@inheritDoc} */
    public boolean readyToTake(ConnState connState) {
        int idleLimit = config.getConnectionIdleLimitInSeconds();
        if (idleLimit >= 0) {
            int idle = (int) (System.currentTimeMillis() - connState.getLastTimeUsedInMillis()) / 1000;
            if (idle >= idleLimit && !executeTestStatement(connState.connection()))
                return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @throws ViburDBCPException if cannot restore the default values for the underlying JDBC Connection.
     */
    public boolean readyToRestore(ConnState connState) throws ViburDBCPException {
        if (config.isResetDefaultsAfterUse())
            setDefaultValues(connState.connection());
        connState.setLastTimeUsedInMillis(System.currentTimeMillis());
        return true;
    }

    private boolean executeTestStatement(Connection connection) {
        Statement statement = null;
        try {
            String testConnectionQuery = config.getTestConnectionQuery();
            if (testConnectionQuery.equals(ViburDBCPConfig.IS_VALID_QUERY))
                return connection.isValid(ViburDBCPConfig.TEST_CONNECTION_TIMEOUT);

            statement = connection.createStatement();
            statement.setQueryTimeout(ViburDBCPConfig.TEST_CONNECTION_TIMEOUT);
            statement.execute(testConnectionQuery);
            statement.close();
            return true;
        } catch (SQLException e) {
            logger.debug("Couldn't validate " + connection, e);
            try {
                if (statement != null) statement.close();
            } catch (SQLException ignore) {
            }
            return false;
        }
    }

    /** {@inheritDoc} */
    public void destroy(ConnState connState) {
        Connection connection = connState.connection();
        logger.trace("Destroying {}", connection);
        try {
            destroyListener.onDestroy(connection);
            connection.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close " + connection, e);
        }
    }
}
