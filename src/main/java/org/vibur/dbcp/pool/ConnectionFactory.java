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
import org.vibur.dbcp.pool.HookHolder.ConnHooksAccessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.vibur.dbcp.util.JdbcUtils.*;

/**
 * The object factory which controls the lifecycle of the underlying JDBC Connections: creates them,
 * validates them if needed, and destroys them. Used by {@link org.vibur.dbcp.ViburDBCPDataSource}.
 *
 * <p>This {@code ConnectionFactory} is a versioned factory which creates versioned JDBC Connection
 * wrappers {@code ConnHolder(s)}. The version of each {@link ConnHolder} created by the factory is the same
 * as the version of the factory at the moment of the object creation.
 *
 * @see Hook
 * @see DefaultHook
 *
 * @author Simeon Malchev
 * @author Daniel Caldeweyher
 */
public class ConnectionFactory implements ViburObjectFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);

    private final ViburConfig config;
    private final ConnHooksAccessor connHooksAccessor;
    private final AtomicInteger version = new AtomicInteger(1);

    /**
     * Instantiates this object factory.
     *
     * @param config the ViburConfig from which will initialize
     * @throws ViburDBCPException if cannot successfully initialize/configure the underlying SQL system
     */
    public ConnectionFactory(ViburConfig config) throws ViburDBCPException {
        this.config = config;
        this.connHooksAccessor = (ConnHooksAccessor) config.getConnHooks();
        initLoginTimeout(config);
    }

    @Override
    public ConnHolder create() throws ViburDBCPException {
        return create(config.getConnector());
    }

    @Override
    public ConnHolder create(Connector connector) throws ViburDBCPException {
        Connection rawConnection = null;
        SQLException sqlException = null;
        long startNanoTime = System.nanoTime();

        try {
            rawConnection = requireNonNull(connector.connect());

        } catch (SQLException e) {
            sqlException = e;
            logger.debug("Couldn't create rawConnection", e);
        }

        return postCreate(rawConnection, sqlException, startNanoTime);
    }

    private ConnHolder postCreate(Connection rawConnection, SQLException sqlException, long startNanoTime) throws ViburDBCPException {
        Hook.InitConnection[] onInit = connHooksAccessor.onInit();
        long currentNanoTime = onInit.length > 0 || config.getConnectionIdleLimitInSeconds() >= 0 ? System.nanoTime() : 0;

        if (onInit.length > 0) {
            try {
                long takenNanos = currentNanoTime - startNanoTime;
                for (Hook.InitConnection hook : onInit)
                    hook.on(rawConnection, takenNanos);

            } catch (SQLException e) {
                quietClose(rawConnection);
                sqlException = chainSQLException(sqlException, e);
            }
        }

        if (sqlException != null)
            throw new ViburDBCPException(sqlException);

        logger.debug("Created rawConnection {}", rawConnection);
        return prepareTracking(new ConnHolder(rawConnection, version(),
                config.getConnectionIdleLimitInSeconds() >= 0 ? currentNanoTime : 0));
    }

    @Override
    public boolean readyToTake(ConnHolder connHolder) {
        if (connHolder.version() != version())
            return false;

        int idleLimit = config.getConnectionIdleLimitInSeconds();
        if (idleLimit >= 0) {
            long idleNanos = System.nanoTime() - connHolder.getRestoredNanoTime();
            if (NANOSECONDS.toSeconds(idleNanos) >= idleLimit
                    && !validateOrInitialize(connHolder.rawConnection(), config.getTestConnectionQuery(), config)) {
                logger.debug("Couldn't validate rawConnection {}", connHolder.rawConnection());
                return false;
            }
        }

        prepareTracking(connHolder);
        return true;
    }

    @Override
    public boolean readyToRestore(ConnHolder connHolder) {
        clearTracking(connHolder); // we don't want to keep the tracking objects references

        Hook.CloseConnection[] onClose = connHooksAccessor.onClose();
        long currentNanoTime = onClose.length > 0 || config.getConnectionIdleLimitInSeconds() >= 0 ? System.nanoTime() : 0;

        if (onClose.length > 0) {
            Connection rawConnection = connHolder.rawConnection();
            try {
                long takenNanos = currentNanoTime - connHolder.getTakenNanoTime();
                for (Hook.CloseConnection hook : onClose)
                    hook.on(rawConnection, takenNanos);

            } catch (SQLException e) {
                logger.debug("Couldn't reset rawConnection {}", rawConnection, e);
                return false;
            }
        }

        if (config.getConnectionIdleLimitInSeconds() >= 0)
            connHolder.setRestoredNanoTime(currentNanoTime);
        return true;
    }

    private ConnHolder prepareTracking(ConnHolder connHolder) {
        if (config.isPoolEnableConnectionTracking()) {
            connHolder.setTakenNanoTime(System.nanoTime());
            connHolder.setThread(Thread.currentThread());
            connHolder.setLocation(new Throwable());
        }
        else if (connHooksAccessor.onGet().length > 0 || connHooksAccessor.onClose().length > 0)
            connHolder.setTakenNanoTime(System.nanoTime());

        return connHolder;
    }

    private void clearTracking(ConnHolder connHolder) {
        if (config.isPoolEnableConnectionTracking()) {
            connHolder.setTakenNanoTime(0);
            connHolder.setLastAccessNanoTime(0);
            connHolder.setProxyConnection(null);
            connHolder.setThread(null);
            connHolder.setLocation(null);
        }
    }

    @Override
    public void destroy(ConnHolder connHolder) {
        Connection rawConnection = connHolder.rawConnection();
        logger.debug("Destroying rawConnection {}", rawConnection);
        closeStatements(rawConnection);

        Hook.DestroyConnection[] onDestroy = connHooksAccessor.onDestroy();
        long startTime = onDestroy.length == 0 ? 0 : System.nanoTime();

        quietClose(rawConnection);
        long takenNanos = onDestroy.length == 0 ? 0 : System.nanoTime() - startTime;
        for (Hook.DestroyConnection hook : onDestroy)
            hook.on(rawConnection, takenNanos);
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
