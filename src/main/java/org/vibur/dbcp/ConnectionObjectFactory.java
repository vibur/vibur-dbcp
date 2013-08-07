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
import vibur.object_pool.PoolObjectFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * @author Simeon Malchev
 */
public class ConnectionObjectFactory implements PoolObjectFactory<Connection> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionObjectFactory.class);

    /** Database driver class name */
    private final String driverClassName;
    /** Database JDBC Connection string. */
    private final String jdbcUrl;
    /** User name to use. */
    private final String username;
    /** Password to use. */
    private final String password;


    /** If set to true, will validate the taken from the pool JDBC Connections before to give them to
     * the application. */
    private final boolean validateOnTake;
    /** If set to true, will validate the returned by the application JDBC Connection before to restore
     * it in the pool. */
    private final boolean validateOnRestore;
    /** Used to test the validity of the JDBC Connection. Set to {@code null} to disable. */
    private final String testConnectionQuery;


    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * wait for this value before attempting to acquire a new JDBC Connection again. */
    private final long acquireRetryDelayInMs;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * try to connect these many times before giving up. */
    private final int acquireRetryAttempts;


    /** The default auto-commit state of created connections. */
    private final Boolean defaultAutoCommit;
    /** The default read-only state of created connections. */
    private final Boolean defaultReadOnly;
    /** The default transaction isolation state of created connections. */
    private final Integer defaultTransactionIsolation;
    /** The default catalog state of created connections. */
    private final String defaultCatalog;

    private final DestroyListener destroyListener;

    public ConnectionObjectFactory(String driverClassName, String jdbcUrl,
                                   String username, String password,
                                   boolean validateOnTake, boolean validateOnRestore,
                                   String testConnectionQuery,
                                   long acquireRetryDelayInMs, int acquireRetryAttempts,
                                   Boolean defaultAutoCommit, Boolean defaultReadOnly,
                                   Integer defaultTransactionIsolation, String defaultCatalog,
                                   DestroyListener destroyListener) {
        this.driverClassName = driverClassName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.validateOnTake = validateOnTake;
        this.validateOnRestore = validateOnRestore;
        this.testConnectionQuery = testConnectionQuery;
        this.acquireRetryDelayInMs = acquireRetryDelayInMs;
        this.acquireRetryAttempts = acquireRetryAttempts;
        this.defaultAutoCommit = defaultAutoCommit;
        this.defaultReadOnly = defaultReadOnly;
        this.defaultTransactionIsolation = defaultTransactionIsolation;
        this.defaultCatalog = defaultCatalog;
        this.destroyListener = destroyListener;

        try {
            Class.forName(this.driverClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws ViburDBCPException if cannot create the underlying JDBC Connection.
     * */
    public Connection create() {
        int retry = 0;
        while (true) {
            try {
                Connection connection;
                if (username == null && password == null)
                    connection = DriverManager.getConnection(jdbcUrl);
                else
                    connection = DriverManager.getConnection(jdbcUrl, username, password);

                if (defaultAutoCommit != null)
                    connection.setAutoCommit(defaultAutoCommit);
                if (defaultReadOnly != null)
                    connection.setReadOnly(defaultReadOnly);
                if (defaultTransactionIsolation != null)
                    connection.setTransactionIsolation(defaultTransactionIsolation);
                if (defaultCatalog != null)
                    connection.setCatalog(defaultCatalog);

                logger.trace("Created " + connection);
                return connection;
            } catch (SQLException e) {
                logger.debug("Couldn't create a java.sql.Connection, attempt " + retry, e);
                if (retry++ >= acquireRetryAttempts)
                    throw new ViburDBCPException(e);
                try {
                    TimeUnit.MILLISECONDS.sleep(acquireRetryDelayInMs);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    /** {@inheritDoc} */
    public boolean readyToTake(Connection connection) {
        return !validateOnTake || executeTestStatement(connection);
    }

    /** {@inheritDoc} */
    public boolean readyToRestore(Connection connection) {
        return !validateOnRestore || executeTestStatement(connection);
    }

    private boolean executeTestStatement(Connection connection) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
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
    public void destroy(Connection connection) {
        try {
            logger.trace("Destroying " + connection);

            destroyListener.onDestroy(connection);
            connection.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close " + connection, e);
        }
    }
}
