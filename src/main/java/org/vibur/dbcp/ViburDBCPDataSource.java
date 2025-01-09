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
import org.vibur.dbcp.pool.ConnectionFactory;
import org.vibur.dbcp.pool.DefaultHook;
import org.vibur.dbcp.pool.PoolOperations;
import org.vibur.dbcp.pool.TakenConnection;
import org.vibur.dbcp.pool.TakenConnectionsFormatter;
import org.vibur.dbcp.pool.ViburListener;
import org.vibur.dbcp.stcache.ConcurrentStatementCache;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.util.ThreadedPoolReducer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.vibur.dbcp.ViburDataSource.State.NEW;
import static org.vibur.dbcp.ViburDataSource.State.TERMINATED;
import static org.vibur.dbcp.ViburDataSource.State.WORKING;
import static org.vibur.dbcp.ViburMonitoring.registerMBean;
import static org.vibur.dbcp.ViburMonitoring.unregisterMBean;
import static org.vibur.dbcp.pool.Connector.Builder.buildConnector;
import static org.vibur.dbcp.pool.ViburListener.NO_TAKEN_CONNECTIONS;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;
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

    public interface ConnectionInvalidator { // for internal use only
        void invalidate();
    }

    private static final Logger logger = LoggerFactory.getLogger(ViburDBCPDataSource.class);

    private final AtomicReference<State> state = new AtomicReference<>(NEW);

    private PoolOperations poolOperations;

    /**
     * Default constructor for programmatic configuration via the {@code ViburConfig}
     * setter methods.
     */
    public ViburDBCPDataSource() {
    }

    /**
     * Initialization via properties file name. Must be either standard properties file
     * or XML file which is compliant with <a href="http://java.sun.com/dtd/properties.dtd">the standard</a>.
     *
     * <p>{@code configFileName} can be {@code null} in which case the default resource
     * file names {@link #DEFAULT_XML_CONFIG_FILE_NAME} or {@link #DEFAULT_PROPERTIES_CONFIG_FILE_NAME}
     * will be loaded, in this order.
     *
     * @param configFileName the properties config file name
     * @throws ViburDBCPException if cannot configure this DataSource successfully
     */
    public ViburDBCPDataSource(String configFileName) throws ViburDBCPException {
        URL config;
        if (configFileName != null) {
            config = getURL(configFileName);
            if (config == null) {
                throw new ViburDBCPException("Unable to load resource " + configFileName);
            }
        }
        else {
            config = getURL(DEFAULT_XML_CONFIG_FILE_NAME);
            if (config == null) {
                config = getURL(DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
                if (config == null) {
                    throw new ViburDBCPException("Unable to load default resources from "
                        + DEFAULT_XML_CONFIG_FILE_NAME + " or " + DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
                }
            }
        }
        configureFromURL(config);
    }

    /**
     * Initialization via the given properties.
     *
     * @param properties the given properties
     * @throws ViburDBCPException if cannot configure this DataSource successfully
     */
    public ViburDBCPDataSource(Properties properties) throws ViburDBCPException {
        configureFromProperties(properties);
    }

    private URL getURL(String configFileName) {
        var config = Thread.currentThread().getContextClassLoader().getResource(configFileName);
        if (config == null) {
            config = getClass().getClassLoader().getResource(configFileName);
            if (config == null) {
                config = ClassLoader.getSystemResource(configFileName);
            }
        }
        return config;
    }

    private void configureFromURL(URL config) throws ViburDBCPException {
        InputStream inputStream = null;
        try {
            var uConn = config.openConnection();
            uConn.setUseCaches(false);
            inputStream = uConn.getInputStream();
            var properties = new Properties();
            if (config.getFile().endsWith(".xml")) {
                properties.loadFromXML(inputStream);
            }
            else {
                properties.load(inputStream);
            }
            configureFromProperties(properties);
        } catch (IOException e) {
            throw new ViburDBCPException(config.toString(), e);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                logger.debug("Couldn't close configuration URL {}", config, e);
            }
        }
    }

    private void configureFromProperties(Properties properties) throws ViburDBCPException {
        Set<String> fields = new HashSet<>();
        for (var field : ViburConfig.class.getDeclaredFields()) {
            fields.add(field.getName());
        }

        for (var entry : properties.entrySet()) {
            var key = (String) entry.getKey();
            var val = (String) entry.getValue();
            if (!fields.contains(key)) {
                logger.warn("Ignoring unknown configuration property {}", key);
                continue;
            }
            try {
                var field = ViburConfig.class.getDeclaredField(key);
                var type = field.getType();
                if (type == int.class || type == Integer.class) {
                    set(field, parseInt(val));
                }
                else if (type == long.class || type == Long.class) {
                    set(field, parseLong(val));
                }
                else if (type == float.class || type == Float.class) {
                    set(field, parseFloat(val));
                }
                else if (type == boolean.class || type == Boolean.class) {
                    set(field, parseBoolean(val));
                }
                else if (type == String.class) {
                    set(field, val);
                }
                else {
                    throw new ViburDBCPException(format("Unexpected type for configuration property %s/%s", key, val));
                }
            } catch (IllegalArgumentException | ReflectiveOperationException e) {
                throw new ViburDBCPException(format("Error setting configuration property %s/%s", key, val), e);
            }
        }
    }

    private void set(Field field, Object value) throws IllegalArgumentException, ReflectiveOperationException {
        var filedName = field.getName();
        var methodSetter = "set" + filedName.substring(0, 1).toUpperCase() + filedName.substring(1);
        var setter = ViburConfig.class.getDeclaredMethod(methodSetter, field.getType());
        setter.invoke(this, value);
    }

    /**
     * {@inheritDoc}
     *
     * @throws ViburDBCPException if not in a {@code NEW} state when started;
     *      if a configuration error is found during start;
     *      if cannot start this DataSource successfully, that is, if cannot successfully
     *      initialize/configure the underlying SQL system, if cannot create the underlying SQL connections,
     *      if cannot initialize the configured/needed JDBC Driver, if cannot create the configured pool reducer,
     *      or if cannot initialize JMX
     */
    @Override
    public void start() throws ViburDBCPException {
        try {
            doStart();
            logger.info("Started {}", this);
        } catch (IllegalStateException e) {
            throw new ViburDBCPException(e);
        } catch (IllegalArgumentException | NullPointerException | ViburDBCPException e) {
            logger.error("Unable to start {} due to:", this, e);
            terminate();
            throw e instanceof ViburDBCPException ? e : new ViburDBCPException(e);
        }
    }

    private void doStart() throws ViburDBCPException {
        if (!state.compareAndSet(NEW, WORKING)) {
            throw new IllegalStateException();
        }

        validateConfig();

        if (getExternalDataSource() == null) {
            initJdbcDriver();
        }
        if (getConnector() == null) {
            setConnector(buildConnector(this, getUsername(), getPassword()));
        }

        initDefaultHooks();

        var connectionFactory = getConnectionFactory();
        if (connectionFactory == null) {
            setConnectionFactory(connectionFactory = new ConnectionFactory(this));
        }
        var pool = getPool();
        if (pool == null) {
            if (isPoolEnableConnectionTracking() && getTakenConnectionsFormatter() == null) {
                setTakenConnectionsFormatter(new TakenConnectionsFormatter.Default(this));
            }

            pool = new ConcurrentPool<>(getConcurrentCollection(), connectionFactory,
                    getPoolInitialSize(), getPoolMaxSize(), isPoolFair(),
                    isPoolEnableConnectionTracking() ? new ViburListener(this) : null);
            setPool(pool);
        }
        poolOperations = new PoolOperations(this, connectionFactory, pool);

        initPoolReducer();
        initStatementCache();

        if (isEnableJMX()) {
            registerMBean(this);
        }
    }

    @Override
    public void terminate() {
        var oldState = state.getAndSet(TERMINATED);
        if (oldState == TERMINATED || oldState == NEW) {
            return;
        }

        if (getPool() != null) {
            getPool().terminate();
        }
        var takenConnections = getTakenConnections();

        if (getPoolReducer() != null) {
            getPoolReducer().terminate();
        }
        if (getStatementCache() != null) {
            getStatementCache().close();
        }

        if (isEnableJMX()) {
            unregisterMBean(this);
        }

        if (!isPoolEnableConnectionTracking()) {
            logger.info("Terminated {}", this);
        }
        else {
            logger.info("Terminated {}, remaining taken connections {}", this, Arrays.deepToString(takenConnections));
        }
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
        forbidIllegalArgument(getStatementCacheMaxSize() < 0 && getStatementCache() == null);
        forbidIllegalArgument(getReducerTimeIntervalInSeconds() > 0 && getPoolReducerClass() == null && getPoolReducer() == null);
        forbidIllegalArgument(getReducerSamples() <= 0);
        forbidIllegalArgument(getConnectionIdleLimitInSeconds() >= 0 && getTestConnectionQuery() == null);
        forbidIllegalArgument(getValidateTimeoutInSeconds() < 0);
        forbidIllegalArgument(isUseNetworkTimeout() && getNetworkTimeoutExecutor() == null);
        requireNonNull(getCriticalSQLStates());

        if (getPassword() == null) {
            logger.warn("JDBC password is not specified.");
        }
        if (getUsername() == null) {
            logger.warn("JDBC username is not specified.");
        }

        var connectionTimeoutInSeconds = (int) MILLISECONDS.toSeconds(getConnectionTimeoutInMs());
        if (getLoginTimeoutInSeconds() > connectionTimeoutInSeconds) {
            logger.info("Setting loginTimeoutInSeconds to {}", connectionTimeoutInSeconds);
            setLoginTimeoutInSeconds(connectionTimeoutInSeconds);
        }
        if (getLogConnectionLongerThanMs() > getConnectionTimeoutInMs()) {
            logger.info("Setting logConnectionLongerThanMs to {}", getConnectionTimeoutInMs());
            setLogConnectionLongerThanMs(getConnectionTimeoutInMs());
        }
        if (isLogTakenConnectionsOnTimeout() && !isPoolEnableConnectionTracking()) {
            logger.info("Setting poolEnableConnectionTracking to true");
            setPoolEnableConnectionTracking(true);
        }
        if (getStatementCacheMaxSize() > STATEMENT_CACHE_MAX_SIZE) {
            logger.info("Setting statementCacheMaxSize to {}", STATEMENT_CACHE_MAX_SIZE);
            setStatementCacheMaxSize(STATEMENT_CACHE_MAX_SIZE);
        }

        if (getDefaultTransactionIsolation() != null) {
            var defaultTransactionIsolation = getDefaultTransactionIsolation().toUpperCase();
            switch (defaultTransactionIsolation) {
                case "NONE" :
                    setDefaultTransactionIsolationIntValue(TRANSACTION_NONE);
                    break;
                case "READ_COMMITTED" :
                    setDefaultTransactionIsolationIntValue(TRANSACTION_READ_COMMITTED);
                    break;
                case "REPEATABLE_READ" :
                    setDefaultTransactionIsolationIntValue(TRANSACTION_REPEATABLE_READ);
                    break;
                case "READ_UNCOMMITTED" :
                    setDefaultTransactionIsolationIntValue(TRANSACTION_READ_UNCOMMITTED);
                    break;
                case "SERIALIZABLE" :
                    setDefaultTransactionIsolationIntValue(TRANSACTION_SERIALIZABLE);
                    break;
                default:
                    logger.warn("Unknown defaultTransactionIsolation {}. Will use the driver's default.",
                            getDefaultTransactionIsolation());
            }
        }
    }

    private void initJdbcDriver() throws ViburDBCPException {
        if (getDriver() == null) {
            try {
                if (getDriverClassName() != null) {
                    setDriver((Driver) Class.forName(getDriverClassName()).getDeclaredConstructor().newInstance());
                }
                else {
                    setDriver(DriverManager.getDriver(getJdbcUrl()));
                }
            } catch (ReflectiveOperationException | ClassCastException | SQLException e) {
                throw new ViburDBCPException(e);
            }
        }
    }

    private void initDefaultHooks() {
        getConnHooks().addOnInit(new DefaultHook.InitConnection(this));
        getConnHooks().addOnGet(new DefaultHook.GetConnectionTiming(this));
        getConnHooks().addOnClose(new DefaultHook.CloseConnection(this));
        getConnHooks().addOnTimeout(new DefaultHook.GetConnectionTimeout(this));

        getInvocationHooks().addOnStatementExecution(new DefaultHook.QueryTiming(this));
        getInvocationHooks().addOnResultSetRetrieval(new DefaultHook.ResultSetSize(this));
    }

    private void initPoolReducer() throws ViburDBCPException {
        var poolReducer = getPoolReducer();
        if (getReducerTimeIntervalInSeconds() > 0 && poolReducer == null) {
            try {
                poolReducer = (ThreadedPoolReducer) Class.forName(getPoolReducerClass())
                        .getConstructor(ViburConfig.class).newInstance(this);
                setPoolReducer(poolReducer);
                poolReducer.start();
            } catch (ReflectiveOperationException | ClassCastException e) {
                throw new ViburDBCPException(e);
            }
        }
    }

    private void initStatementCache() {
        var statementCacheMaxSize = getStatementCacheMaxSize();
        if (statementCacheMaxSize > 0 && getStatementCache() == null) {
            setStatementCache(new ConcurrentStatementCache(statementCacheMaxSize));
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        var state = validatePoolState(isAllowConnectionAfterTermination());
        if (state == WORKING) {
            try {
                return poolOperations.getProxyConnection(getConnectionTimeoutInMs());
            } catch (SQLException e) {
                if (!SQLSTATE_POOL_CLOSED_ERROR.equals(e.getSQLState()) || !isAllowConnectionAfterTermination()) {
                    throw e;
                }
                // else falls back to creating a non-pooled Connection
                logger.info("The pool was closed while retrieving a Connection.");
            }
        }

        assert getState() == TERMINATED;
        logger.info("Calling getConnection() after the pool was closed; will create and return a non-pooled Connection.");
        return getNonPooledConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (defaultCredentials(username, password)) {
            return getConnection();
        }

        validatePoolState(isAllowConnectionAfterTermination());
        logger.warn("Calling getConnection() with different than the default credentials; will create and return a non-pooled Connection.");
        return getNonPooledConnection(username, password);
    }

    @Override
    public Connection getNonPooledConnection() throws SQLException {
        return getNonPooledConnection(getUsername(), getPassword());
    }

    @Override
    public Connection getNonPooledConnection(String username, String password) throws SQLException {
        validatePoolState(true);
        try {
            var connector = buildConnector(this, username, password);
            var rawConnection = getConnectionFactory().create(connector).rawConnection();
            logger.debug("Taking non-pooled rawConnection {}", rawConnection);
            return rawConnection;
        } catch (ViburDBCPException e) {
            throw e.unwrapSQLException();
        }
    }

    @Override
    public void severConnection(Connection connection) throws SQLException {
        if (Proxy.isProxyClass(connection.getClass())) {
            var ih = Proxy.getInvocationHandler(connection);
            if (ih instanceof ConnectionInvalidator) {
                ((ConnectionInvalidator) ih).invalidate();
                return;
            }
        }
        connection.close();
    }

    private State validatePoolState(boolean allowConnectionAfterTermination) throws SQLException {
        var state = getState();
        switch (state) {
            case NEW:
                throw new SQLException(format("Pool %s, %s", getName(), state), SQLSTATE_POOL_NOTSTARTED_ERROR);
            case WORKING:
                return state;
            case TERMINATED:
                if (!allowConnectionAfterTermination) {
                    throw new SQLException(format("Pool %s, %s", getPoolName(this), state), SQLSTATE_POOL_CLOSED_ERROR);
                }
                return state;
            default:
                throw new AssertionError(state);
        }
    }

    private boolean defaultCredentials(String username, String password) {
        if (getUsername() != null ? !getUsername().equals(username) : username != null) {
            return false;
        }
        return getPassword() != null ? getPassword().equals(password) : password == null;
    }

    @Override
    public String getTakenConnectionsStackTraces() {
        if (!isPoolEnableConnectionTracking()) {
            return "poolEnableConnectionTracking is disabled or the pool is not in working state";
        }

        return getTakenConnectionsFormatter().formatTakenConnections(getTakenConnections());
    }

    @Override
    public TakenConnection[] getTakenConnections() {
        if (!isPoolEnableConnectionTracking()) {
            return NO_TAKEN_CONNECTIONS;
        }

        return ((ViburListener) getPool().listener()).getTakenConnections();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
        setLoginTimeoutInSeconds(seconds);
    }

    @Override
    public int getLoginTimeout() {
        return getLoginTimeoutInSeconds();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) getExternalDataSource();
        }
        throw new SQLException("Not a wrapper for " + iface, SQLSTATE_WRAPPER_ERROR);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return isAllowUnwrapping() && iface.isInstance(getExternalDataSource());
    }
}
