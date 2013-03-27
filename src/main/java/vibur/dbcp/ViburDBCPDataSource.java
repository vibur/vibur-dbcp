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

package vibur.dbcp;

import org.slf4j.LoggerFactory;
import vibur.dbcp.cache.ConcurrentCache;
import vibur.dbcp.cache.ConcurrentFifoCache;
import vibur.dbcp.listener.DestroyListener;
import vibur.dbcp.proxy.Proxy;
import vibur.dbcp.proxy.cache.StatementKey;
import vibur.object_pool.ConcurrentHolderLinkedPool;
import vibur.object_pool.Holder;
import vibur.object_pool.HolderValidatingPoolService;
import vibur.object_pool.PoolObjectFactory;
import vibur.object_pool.util.DefaultReducer;
import vibur.object_pool.util.PoolReducer;
import vibur.object_pool.util.Reducer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPDataSource extends ViburDBCPConfig implements DataSource, DestroyListener {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ViburDBCPDataSource.class);

    private static final int CACHE_MAX_SIZE = 500;

    private PrintWriter logWriter = null;

    private PoolObjectFactory<Connection> connectionObjectFactory;
    private HolderValidatingPoolService<Connection> connectionPool;
    private Reducer reducer;
    private PoolReducer poolReducer;

    public enum State {
        NEW,
        WORKING,
        TERMINATED
    }

    private State state = State.NEW;

    public ViburDBCPDataSource() { // default constructor
    }

    public synchronized void start() {
        if (state != State.NEW)
            throw new IllegalStateException();
        state = State.WORKING;

        validateConfig();

        connectionObjectFactory = new ConnectionObjectFactory(
            getDriverClassName(), getJdbcUrl(),
            getUsername(), getPassword(),
            isValidateOnTake(), isValidateOnRestore(), getTestConnectionQuery(),
            getAcquireRetryDelayInMs(), getAcquireRetryAttempts(),
            getDefaultAutoCommit(), getDefaultReadOnly(),
            getDefaultTransactionIsolationValue(), getDefaultCatalog(),
            this);
        connectionPool = new ConcurrentHolderLinkedPool<Connection>(connectionObjectFactory,
            getPoolInitialSize(), getPoolMaxSize(), isPoolFair(), isPoolEnableConnectionTracking());

        reducer = new DefaultReducer(getReducerTakenRatio(), getReducerReduceRatio());
        poolReducer = new PoolReducer(connectionPool, reducer,
            getReducerTimeoutInSeconds(), TimeUnit.SECONDS) {

            protected void afterReduce(int reduction, int reduced, Throwable thrown) {
                if (thrown != null)
                    logger.error("{} thrown while intending to reduce by {}", thrown, reduction);
                else
                    logger.debug("Intended reduction {} actual {}", reduction, reduced);
            }
        };

        int statementCacheMaxSize = getStatementCacheMaxSize();
        if (statementCacheMaxSize > CACHE_MAX_SIZE)
            statementCacheMaxSize = CACHE_MAX_SIZE;
        if (statementCacheMaxSize > 0)
            setStatementCache(new ConcurrentFifoCache<StatementKey, Statement>
                (statementCacheMaxSize));
    }

    public synchronized void shutdown() {
        if (state == State.TERMINATED) return;
        if (state != State.WORKING)
            throw new IllegalStateException();

        ConcurrentCache<StatementKey, Statement> statementCache = getStatementCache();
        if (statementCache != null)
            statementCache.clear();
        poolReducer.terminate();
        connectionPool.terminate();
    }

    private void validateConfig() {
        if (getDriverClassName() == null || getJdbcUrl() == null
            || getCreateConnectionTimeoutInMs() < 0 || getAcquireRetryDelayInMs() < 0
            || getAcquireRetryAttempts() < 0 || getQueryExecuteTimeLimitInMs() < 0
            || getStatementCacheMaxSize() < 0
            || (getTestConnectionQuery() == null && (isValidateOnTake() || isValidateOnRestore())))
            throw new IllegalArgumentException();

        if (getPassword() == null) logger.warn("JDBC password not specified.");
        if (getUsername() == null) logger.warn("JDBC username not specified.");

        if (getDefaultTransactionIsolation() != null) {
            String defaultTransactionIsolation = getDefaultTransactionIsolation().toUpperCase();

            if (defaultTransactionIsolation.equals("NONE")) {
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_NONE);
            } else if (defaultTransactionIsolation.equals("READ_COMMITTED")) {
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_READ_COMMITTED);
            } else if (defaultTransactionIsolation.equals("REPEATABLE_READ")) {
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_REPEATABLE_READ);
            } else if (defaultTransactionIsolation.equals("READ_UNCOMMITTED")) {
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_READ_UNCOMMITTED);
            } else if (defaultTransactionIsolation.equals("SERIALIZABLE")) {
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_SERIALIZABLE);
            } else {
                logger.warn("Unknown defaultTransactionIsolation {}. Will use the driver's default.",
                    getDefaultTransactionIsolation());
            }
        }
    }

    public Connection getConnection() throws SQLException {
        return getConnection(getCreateConnectionTimeoutInMs());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        throw new UnsupportedOperationException(
            "Having different usernames/passwords is not supported by this DataSource.");
    }

    private Connection getConnection(long timeout) throws SQLException {
        Holder<Connection> hConnection = timeout == 0 ?
            connectionPool.take() : connectionPool.tryTake(timeout, TimeUnit.MILLISECONDS);
        if (hConnection == null)
            throw new SQLException("Couldn't obtain SQL connection.");
        return Proxy.newConnection(hConnection, connectionPool, this);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        setCreateConnectionTimeoutInMs(seconds * 1000);
    }

    public int getLoginTimeout() throws SQLException {
        return (int) getCreateConnectionTimeoutInMs() / 1000;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("not a wrapper");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void onDestroy(Connection connection) {
        ConcurrentCache<StatementKey, Statement> statementCache = getStatementCache();
        if (statementCache != null)
            for (StatementKey key : statementCache.keySet())
                if (key.getProxy().equals(connection))
                    statementCache.remove(key);
    }

    public State getState() {
        return state;
    }

    public PoolObjectFactory<Connection> getConnectionObjectFactory() {
        return connectionObjectFactory;
    }

    public HolderValidatingPoolService<Connection> getConnectionPool() {
        return connectionPool;
    }

    public Reducer getReducer() {
        return reducer;
    }

    public PoolReducer getPoolReducer() {
        return poolReducer;
    }
}
