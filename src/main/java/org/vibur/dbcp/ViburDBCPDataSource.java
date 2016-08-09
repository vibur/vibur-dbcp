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

import org.slf4j.LoggerFactory;
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.ConnectionFactory;
import org.vibur.dbcp.pool.PoolOperations;
import org.vibur.dbcp.proxy.ConnectionInvocationHandler;
import org.vibur.objectpool.ConcurrentLinkedPool;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.TakenListener;
import org.vibur.objectpool.util.ThreadedPoolReducer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.sql.Connection.*;
import static java.util.Objects.requireNonNull;
import static org.vibur.dbcp.ViburDataSource.State.*;
import static org.vibur.dbcp.ViburMonitoring.registerMBean;
import static org.vibur.dbcp.ViburMonitoring.unregisterMBean;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
import static org.vibur.dbcp.util.ViburUtils.unwrapSQLException;
import static org.vibur.objectpool.util.ArgumentValidation.forbidIllegalArgument;

/**
 * The main DataSource which needs to be configured/instantiated by the calling application and from
 * which the JDBC Connections will be obtained via calling the {@link #getConnection()} method. The
 * lifecycle operations of this DataSource, as well as the other specific to it operations, are
 * defined by the {@link ViburDataSource} interface.
 *
 * @see javax.sql.DataSource
 * @see ConnectionFactory
 *
 * @author Simeon Malchev
 */
public class ViburDBCPDataSource extends ViburConfig implements ViburDataSource {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ViburDBCPDataSource.class);

    private final AtomicReference<State> state = new AtomicReference<>(NEW);

    private ConnectionFactory connectionFactory;
    private PoolOperations poolOperations;
    private ThreadedPoolReducer poolReducer = null;

    private PrintWriter logWriter = null;

    /**
     * Default constructor for programmatic configuration via the {@code ViburConfig}
     * setter methods.
     */
    public ViburDBCPDataSource() {
    }

    /**
     * Initialization via properties file name. Must be either standard properties file
     * or XML file which is complaint with "http://java.sun.com/dtd/properties.dtd".
     *
     * <p>{@code configFileName} can be {@code null} in which case the default resource
     * file names {@link #DEFAULT_XML_CONFIG_FILE_NAME} or {@link #DEFAULT_PROPERTIES_CONFIG_FILE_NAME}
     * will be loaded, in this order.
     *
     * @param configFileName the properties config file name
     * @throws ViburDBCPException if cannot configure successfully
     */
    public ViburDBCPDataSource(String configFileName) throws ViburDBCPException {
        URL config;
        if (configFileName != null) {
            config = getURL(configFileName);
            if (config == null)
                throw new ViburDBCPException("Unable to load resource " + configFileName);
        }
        else {
            config = getURL(DEFAULT_XML_CONFIG_FILE_NAME);
            if (config == null) {
                config = getURL(DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
                if (config == null)
                    throw new ViburDBCPException("Unable to load default resources from "
                        + DEFAULT_XML_CONFIG_FILE_NAME + " or " + DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
            }
        }
        configureFromURL(config);
    }

    /**
     * Initialization via the given properties.
     *
     * @param properties the given properties
     * @throws ViburDBCPException if cannot configure successfully
     */
    public ViburDBCPDataSource(Properties properties) throws ViburDBCPException {
        configureFromProperties(properties);
    }

    private URL getURL(String configFileName) {
        URL config = Thread.currentThread().getContextClassLoader().getResource(configFileName);
        if (config == null) {
            config = getClass().getClassLoader().getResource(configFileName);
            if (config == null)
                config = ClassLoader.getSystemResource(configFileName);
        }
        return config;
    }

    private void configureFromURL(URL config) throws ViburDBCPException {
        InputStream inputStream = null;
        try {
            URLConnection uConn = config.openConnection();
            uConn.setUseCaches(false);
            inputStream = uConn.getInputStream();
            Properties properties = new Properties();
            if (config.getFile().endsWith(".xml"))
                properties.loadFromXML(inputStream);
            else
                properties.load(inputStream);
            configureFromProperties(properties);
        } catch (IOException e) {
            throw new ViburDBCPException(config.toString(), e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                    logger.debug("Couldn't close configuration URL {}", config, ignored);
                }
            }
        }
    }

    private void configureFromProperties(Properties properties) throws ViburDBCPException {
        Set<String> fields = new HashSet<>();
        for (Field field : ViburConfig.class.getDeclaredFields())
            fields.add(field.getName());

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            try {
                if (!fields.contains(key)) {
                    logger.warn("Unknown configuration property {}", key);
                    continue;
                }
                Field field = ViburConfig.class.getDeclaredField(key);
                Class<?> type = field.getType();
                if (type == int.class || type == Integer.class)
                    set(field, parseInt(val));
                else if (type == long.class || type == Long.class)
                    set(field, parseLong(val));
                else if (type == float.class || type == Float.class)
                    set(field, parseFloat(val));
                else if (type == boolean.class || type == Boolean.class)
                    set(field, parseBoolean(val));
                else if (type == String.class)
                    set(field, val);
                else
                    throw new ViburDBCPException("Unexpected type for configuration property " + key);
            } catch (NumberFormatException | ReflectiveOperationException e) {
                throw new ViburDBCPException(format("Error setting configuration property %s, value = %s", key, val), e);
            }
        }
    }

    private void set(Field field, Object value) throws IllegalAccessException {
        field.setAccessible(true);
        field.set(this, value);
    }

    /**
     * {@inheritDoc}
     *
     * @throws ViburDBCPException if not in a {@code NEW} state when started;
     *      if a configuration error is found during start;
     *      if cannot start this DataSource successfully, that is, if cannot successfully
     *      initialize/configure the underlying SQL system, if cannot create the underlying SQL connections,
     *      if cannot create the configured pool reducer, or if cannot initialize JMX
     */
    @Override
    public void start() throws ViburDBCPException {
        try {
            doStart();
        } catch (IllegalStateException | IllegalArgumentException | NullPointerException e) {
            throw new ViburDBCPException(e);
        }
    }

    private void doStart() throws ViburDBCPException {
        if (!state.compareAndSet(NEW, WORKING))
            throw new IllegalStateException();

        validateConfig();

        connectionFactory = new ConnectionFactory(this);
        PoolService<ConnHolder> poolService = new ConcurrentLinkedPool<>(connectionFactory,
                getPoolInitialSize(), getPoolMaxSize(), isPoolFair(),
                isPoolEnableConnectionTracking() ? new TakenListener<ConnHolder>(getPoolInitialSize()) : null,
                isPoolFifo());
        poolOperations = new PoolOperations(connectionFactory, poolService, this);

        setPool(poolService);
        initPoolReducer();
        initStatementCache();

        if (isEnableJMX())
            registerMBean(this);
        logger.info("Started {}", this);
    }

    @Override
    public void terminate() {
        State oldState = state.getAndSet(TERMINATED);
        if (oldState == TERMINATED || oldState == NEW)
            return;

        if (getStatementCache() != null)
            getStatementCache().close();
        if (poolReducer != null)
            poolReducer.terminate();
        if (getPool() != null)
            getPool().terminate();

        if (isEnableJMX())
            unregisterMBean(this);
        unregisterName();
        logger.info("Terminated {}", this);
    }

    @Override
    public void close() {
        terminate();
    }

    @Override
    public State getState() {
        return state.get();
    }

    private void validateConfig() {
        forbidIllegalArgument(getExternalDataSource() == null && getJdbcUrl() == null);
        forbidIllegalArgument(getAcquireRetryDelayInMs() < 0);
        forbidIllegalArgument(getAcquireRetryAttempts() < 0);
        forbidIllegalArgument(getConnectionTimeoutInMs() < 0);
        forbidIllegalArgument(getLoginTimeoutInSeconds() < 0);
        forbidIllegalArgument(getStatementCacheMaxSize() < 0);
        forbidIllegalArgument(getReducerTimeIntervalInSeconds() < 0);
        forbidIllegalArgument(getReducerTimeIntervalInSeconds() == 0 && getPoolReducerClass() == null);
        forbidIllegalArgument(getReducerSamples() <= 0);
        forbidIllegalArgument(getConnectionIdleLimitInSeconds() >= 0 && getTestConnectionQuery() == null);
        forbidIllegalArgument(getValidateTimeoutInSeconds() < 0);
        forbidIllegalArgument(isUseNetworkTimeout() && getNetworkTimeoutExecutor() == null);
        requireNonNull(getCriticalSQLStates());
        requireNonNull(getViburLogger());

        if (getPassword() == null) logger.warn("JDBC password is not specified.");
        if (getUsername() == null) logger.warn("JDBC username is not specified.");

        if (getLogConnectionLongerThanMs() > getConnectionTimeoutInMs()) {
            logger.warn("Setting logConnectionLongerThanMs to {}", getConnectionTimeoutInMs());
            setLogConnectionLongerThanMs(getConnectionTimeoutInMs());
        }
        if (getStatementCacheMaxSize() > STATEMENT_CACHE_MAX_SIZE) {
            logger.warn("Setting statementCacheMaxSize to {}", STATEMENT_CACHE_MAX_SIZE);
            setStatementCacheMaxSize(STATEMENT_CACHE_MAX_SIZE);
        }

        if (getDefaultTransactionIsolation() != null) {
            String defaultTransactionIsolation = getDefaultTransactionIsolation().toUpperCase();
            switch (defaultTransactionIsolation) {
                case "NONE" :
                    setDefaultTransactionIsolationValue(TRANSACTION_NONE);
                    break;
                case "READ_COMMITTED" :
                    setDefaultTransactionIsolationValue(TRANSACTION_READ_COMMITTED);
                    break;
                case "REPEATABLE_READ" :
                    setDefaultTransactionIsolationValue(TRANSACTION_REPEATABLE_READ);
                    break;
                case "READ_UNCOMMITTED" :
                    setDefaultTransactionIsolationValue(TRANSACTION_READ_UNCOMMITTED);
                    break;
                case "SERIALIZABLE" :
                    setDefaultTransactionIsolationValue(TRANSACTION_SERIALIZABLE);
                    break;
                default:
                    logger.warn("Unknown defaultTransactionIsolation {}. Will use the driver's default.",
                            getDefaultTransactionIsolation());
            }
        }
    }

    private void initPoolReducer() throws ViburDBCPException {
        if (getReducerTimeIntervalInSeconds() > 0) {
            try {
                Object reducer = Class.forName(getPoolReducerClass()).getConstructor(ViburConfig.class)
                        .newInstance(this);
                if (!(reducer instanceof ThreadedPoolReducer))
                    throw new ViburDBCPException(getPoolReducerClass() + " is not an instance of ThreadedPoolReducer");
                poolReducer = (ThreadedPoolReducer) reducer;
                poolReducer.start();
            } catch (ReflectiveOperationException e) {
                throw new ViburDBCPException(e);
            }
        }
    }

    private void initStatementCache() {
        int statementCacheMaxSize = getStatementCacheMaxSize();
        if (statementCacheMaxSize > 0)
            setStatementCache(new StatementCache(statementCacheMaxSize));
    }

    @Override
    public Connection getConnection() throws SQLException {
        State state = validatePoolState(isAllowConnectionAfterTermination());
        if (state == WORKING) {
            try {
                return getPooledConnection(getConnectionTimeoutInMs());
            } catch (SQLException e) {
                if (!e.getSQLState().equals(SQLSTATE_POOL_CLOSED_ERROR) || !isAllowConnectionAfterTermination())
                    throw e;
                // falls back to creating a non-pooled Connection
            }
        }

        assert getState() == TERMINATED;
        logger.info("Calling getConnection() after the pool was closed; will create and return a non-pooled Connection.");
        return getNonPooledConnection();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method will return a <b>raw (non-pooled)</b> JDBC Connection when called with credentials different
     * than the configured default credentials.
     * */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (defaultCredentials(username, password))
            return getConnection();

        validatePoolState(isAllowConnectionAfterTermination());
        logger.warn("Calling getConnection() with different than the default credentials; will create and return a non-pooled Connection.");
        return getNonPooledConnection(username, password);
    }

    private Connection getPooledConnection(long timeout) throws SQLException {
        boolean logSlowConn = getLogConnectionLongerThanMs() >= 0;
        long startTime = logSlowConn ? System.currentTimeMillis() : 0L;

        Connection connProxy = null;
        try {
            return connProxy = poolOperations.getConnection(timeout);
        } finally {
            if (logSlowConn)
                logGetConnection(timeout, startTime, connProxy);
        }
    }

    @Override
    public Connection getNonPooledConnection() throws SQLException {
        return getNonPooledConnection(getUsername(), getPassword());
    }

    @Override
    public Connection getNonPooledConnection(String username, String password) throws SQLException {
        validatePoolState(true);
        try {
            return connectionFactory.create(username, password).value();
        } catch (ViburDBCPException e) {
            return unwrapSQLException(e);
        }
    }

    @Override
    public void severConnection(Connection connection) throws SQLException {
        if (Proxy.isProxyClass(connection.getClass())) {
            InvocationHandler ih = Proxy.getInvocationHandler(connection);
            if (ih instanceof ConnectionInvocationHandler) {
                ConnectionInvocationHandler cih = (ConnectionInvocationHandler) ih;
                cih.invalidate();
                return;
            }
        }
        connection.close();
    }

    private State validatePoolState(boolean allowConnectionAfterTermination) throws SQLException {
        State state = getState();
        switch (state) {
            case NEW:
                throw new SQLException(format("Pool %s, %s", getName(), state), SQLSTATE_POOL_NOTSTARTED_ERROR);
            case WORKING:
                return state;
            case TERMINATED:
                if (!allowConnectionAfterTermination)
                    throw new SQLException(format("Pool %s, %s", getName(), state), SQLSTATE_POOL_CLOSED_ERROR);
                return state;
            default:
                throw new AssertionError(state);
        }
    }

    private boolean defaultCredentials(String username, String password) {
        if (getUsername() != null ? !getUsername().equals(username) : username != null)
            return false;
        return !(getPassword() != null ? !getPassword().equals(password) : password != null);
    }

    private void logGetConnection(long timeout, long startTime, Connection connProxy) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= getLogConnectionLongerThanMs())
            getViburLogger().logGetConnection(getPoolName(this), connProxy, timeout, timeTaken,
                    isLogStackTraceForLongConnection() ? new Throwable().getStackTrace() : null);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        setLoginTimeoutInSeconds(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getLoginTimeoutInSeconds();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("not a wrapper for " + iface, SQLSTATE_WRAPPER_ERROR);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
