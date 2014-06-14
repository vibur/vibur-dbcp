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
import org.vibur.dbcp.cache.MethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.objectpool.PoolObjectFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.vibur.dbcp.util.SqlUtils.closeConnection;
import static org.vibur.dbcp.util.SqlUtils.closeStatement;

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
public class ConnectionFactory implements PoolObjectFactory<ConnHolder>, VersionedObject {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);

    private final ViburDBCPConfig config;
    private final AtomicInteger version = new AtomicInteger(1);

    /**
     * Instantiates this object factory.
     *
     * @param config the ViburDBCPConfig from which will initialize
     * @throws ViburDBCPException if cannot successfully initialize/configure the underlying SQL system
     */
    public ConnectionFactory(ViburDBCPConfig config) throws ViburDBCPException {
        if (config == null)
            throw new NullPointerException();
        this.config = config;

        initLoginTimeout(config);
        initJdbcDriver(config);
    }

    private void initLoginTimeout(ViburDBCPConfig config) throws ViburDBCPException {
        int loginTimeout = config.getLoginTimeoutInSeconds();
        if (config.getExternalDataSource() == null)
            DriverManager.setLoginTimeout(loginTimeout);
        else
            try {
                config.getExternalDataSource().setLoginTimeout(loginTimeout);
            } catch (SQLException e) {
                throw new ViburDBCPException(e);
            }
    }

    private void initJdbcDriver(ViburDBCPConfig config) throws ViburDBCPException {
        if (config.getDriverClassName() != null)
            try {
                Class.forName(config.getDriverClassName()).newInstance();
            } catch (ClassNotFoundException e) {
                throw new ViburDBCPException(e);
            } catch (InstantiationException e) {
                throw new ViburDBCPException(e);
            } catch (IllegalAccessException e) {
                throw new ViburDBCPException(e);
            }
    }

    /**
     * {@inheritDoc}
     *
     * @throws org.vibur.dbcp.ViburDBCPException if cannot create the underlying JDBC Connection.
     */
    public ConnHolder create() throws ViburDBCPException {
        int attempt = 0;
        Connection rawConnection = null;
        while (rawConnection == null) {
            try {
                rawConnection = doCreate();
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

        try {
            ensureConnectionInitialized(rawConnection);
            setDefaultValues(rawConnection);
        } catch (SQLException e) {
            closeConnection(rawConnection);
            throw new ViburDBCPException(e);
        }
        logger.trace("Created {}", rawConnection);
        return new ConnHolder(rawConnection, version(), System.currentTimeMillis());
    }

    private Connection doCreate() throws SQLException {
        Connection connection;
        DataSource externalDataSource = config.getExternalDataSource();
        String userName = config.getUsername();
        if (externalDataSource == null) {
            if (userName != null)
                connection = DriverManager.getConnection(config.getJdbcUrl(), userName, config.getPassword());
            else
                connection = DriverManager.getConnection(config.getJdbcUrl());
        } else {
            if (userName != null)
                connection = externalDataSource.getConnection(userName, config.getPassword());
            else
                connection = externalDataSource.getConnection();
        }
        return connection;
    }

    private void ensureConnectionInitialized(Connection connection) throws SQLException {
        String initSQL = config.getInitSQL();
        if (initSQL != null && !validateConnection(connection, initSQL))
            throw new SQLException("Couldn't validate " + connection);
    }

    private void setDefaultValues(Connection connection) throws SQLException {
        if (config.getDefaultAutoCommit() != null)
            connection.setAutoCommit(config.getDefaultAutoCommit());
        if (config.getDefaultReadOnly() != null)
            connection.setReadOnly(config.getDefaultReadOnly());
        if (config.getDefaultTransactionIsolationValue() != null)
            connection.setTransactionIsolation(config.getDefaultTransactionIsolationValue());
        if (config.getDefaultCatalog() != null)
            connection.setCatalog(config.getDefaultCatalog());
    }

    private boolean validateConnection(Connection connection, String query) throws SQLException {
        if (query.equals(ViburDBCPConfig.IS_VALID_QUERY))
            return connection.isValid(ViburDBCPConfig.QUERY_TIMEOUT);
        else
            return executeQuery(connection, query);
    }

    private boolean executeQuery(Connection connection, String query) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.setQueryTimeout(ViburDBCPConfig.QUERY_TIMEOUT);
            statement.execute(query);
            return true;
        } finally {
            if (statement != null)
                closeStatement(statement);
        }
    }

    /** {@inheritDoc} */
    public boolean readyToTake(ConnHolder conn) {
        if (conn.version() != version())
            return false;

        try {
            int idleLimit = config.getConnectionIdleLimitInSeconds();
            if (idleLimit >= 0) {
                int idle = (int) ((System.currentTimeMillis() - conn.getRestoredTime()) / 1000);
                if (idle >= idleLimit && !validateConnection(conn.value(), config.getTestConnectionQuery()))
                    return false;
            }

            if (config.isPoolEnableConnectionTracking()) {
                conn.setTakenTime(System.currentTimeMillis());
                conn.setStackTrace(new Throwable().getStackTrace());
            }
            return true;
        } catch (SQLException ignored) {
            logger.debug("Couldn't validate " + conn.value(), ignored);
            return false;
        }
    }

    /** {@inheritDoc} */
    public boolean readyToRestore(ConnHolder conn) {
        try {
            if (config.isResetDefaultsAfterUse())
                setDefaultValues(conn.value());
            conn.setRestoredTime(System.currentTimeMillis());
            return true;
        } catch (SQLException ignored) {
            logger.debug("Couldn't set the default values for " + conn.value(), ignored);
            return false;
        }
    }

    /** {@inheritDoc} */
    public void destroy(ConnHolder conn) {
        Connection connection = conn.value();
        logger.trace("Destroying {}", connection);
        closeStatements(connection);
        closeConnection(connection);
    }

    private void closeStatements(Connection connection) {
        ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache = config.getStatementCache();
        if (statementCache == null)
            return;

        for (Iterator<Map.Entry<MethodDef<Connection>, ReturnVal<Statement>>> i =
                     statementCache.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<MethodDef<Connection>, ReturnVal<Statement>> entry = i.next();
            MethodDef<Connection> key = entry.getKey();
            ReturnVal<Statement> value = entry.getValue();
            if (key.getTarget().equals(connection) && statementCache.remove(key, value))
                closeStatement(value.value());
        }
    }

    /** {@inheritDoc} */
    public int version() {
        return version.get();
    }

    /** {@inheritDoc} */
    public boolean compareAndSetVersion(int expect, int update) {
        return version.compareAndSet(expect, update);
    }
}
