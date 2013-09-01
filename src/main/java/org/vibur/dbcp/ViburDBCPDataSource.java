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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.cache.StatementKey;
import org.vibur.dbcp.cache.ValueHolder;
import org.vibur.dbcp.listener.DestroyListener;
import org.vibur.dbcp.proxy.Proxy;
import org.vibur.objectpool.ConcurrentHolderLinkedPool;
import org.vibur.objectpool.Holder;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.util.DefaultReducer;
import org.vibur.objectpool.util.PoolReducer;
import org.vibur.objectpool.util.Reducer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPDataSource extends ViburDBCPConfig implements DataSource, DestroyListener {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ViburDBCPDataSource.class);

    private static final int CACHE_MAX_SIZE = 1000;

    public static final String PROPERTIES_CONFIG_FILE_NAME = "vibur-dbcp-test.properties";
    public static final String XML_CONFIG_FILE_NAME = "vibur-dbcp-test.xml";

    private PrintWriter logWriter = null;

    private PoolObjectFactory<ConnState> connectionObjectFactory;
    private Reducer reducer;
    private PoolReducer poolReducer;

    public enum State {
        NEW,
        WORKING,
        TERMINATED
    }

    private State state = State.NEW;

    /**
     * Default constructor for programmatic configuration via the {@code ViburDBCPConfig}
     * setter methods.
     */
    public ViburDBCPDataSource() {
    }

    /**
     * Initialisation via properties file name. Must be either standard properties file
     * or XML file which is complaint with "http://java.sun.com/dtd/properties.dtd".
     *
     * <p>{@code configFileName} can be {@code null} in which case the default resource
     * file names {@link #XML_CONFIG_FILE_NAME} or {@link #PROPERTIES_CONFIG_FILE_NAME}
     * will be loaded, in this order.
     *
     * @param configFileName the properties config file name
     * @throws ViburDBCPException if cannot configure successfully
     */
    public ViburDBCPDataSource(String configFileName) {
        URL config;
        if (configFileName != null) {
            config = getURL(configFileName);
            if (config == null)
                throw new ViburDBCPException("Unable to load resource " + configFileName);
        } else {
            config = getURL(XML_CONFIG_FILE_NAME);
            if (config == null) {
                config = getURL(PROPERTIES_CONFIG_FILE_NAME);
                if (config == null)
                    throw new ViburDBCPException("Unable to load default resources " + XML_CONFIG_FILE_NAME
                        + " or " + PROPERTIES_CONFIG_FILE_NAME);
            }
        }
        configureFromURL(config);
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

    /**
     * Initialisation via the given properties.
     *
     * @param properties the given properties
     * @throws ViburDBCPException if cannot configure successfully
     */
    public ViburDBCPDataSource(Properties properties) {
        configureFromProperties(properties);
    }

    private void configureFromURL(URL config) {
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            URLConnection uConn = config.openConnection();
            uConn.setUseCaches(false);
            inputStream = uConn.getInputStream();
            if (config.getFile().endsWith(".xml"))
                properties.loadFromXML(inputStream);
            else
                properties.load(inputStream);
            configureFromProperties(properties);
        } catch (IOException e) {
            throw new ViburDBCPException(e);
        } finally {
            if (inputStream != null)
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new ViburDBCPException(e);
                }
        }
    }

    private void configureFromProperties(Properties properties) {
        for (Field field : ViburDBCPConfig.class.getDeclaredFields()) {
            try {
                String val = properties.getProperty(field.getName());
                if (val != null) {
                    Class<?> type = field.getType();
                    if (type == int.class || type == Integer.class)
                        set(field, Integer.parseInt(val));
                    else if (type == long.class || type == Long.class)
                        set(field, Long.parseLong(val));
                    else if (type == float.class || type == Float.class)
                        set(field, Float.parseFloat(val));
                    else if (type == boolean.class || type == Boolean.class)
                        set(field, Boolean.parseBoolean(val));
                    else if (type == String.class)
                        set(field, val);
                }
            } catch (NumberFormatException e) {
                throw new ViburDBCPException(e);
            } catch (IllegalAccessException e) {
                throw new ViburDBCPException(e);
            }
        }
    }

    private void set(Field field, Object value) throws IllegalAccessException {
        field.setAccessible(true);
        field.set(this, value);
    }

    /**
     * Starts this datasource. In order to be used the datasource has to be first initialised
     * via call to one of the constructors and then started via call to this method.
     */
    public synchronized void start() {
        if (state != State.NEW)
            throw new IllegalStateException();
        state = State.WORKING;

        validateConfig();

        connectionObjectFactory = new ConnectionObjectFactory(this, this);
        setConnectionPool(new ConcurrentHolderLinkedPool<ConnState>(connectionObjectFactory,
            getPoolInitialSize(), getPoolMaxSize(), isPoolFair(), isPoolEnableConnectionTracking()));

        reducer = new DefaultReducer(getReducerTakenRatio(), getReducerReduceRatio());
        poolReducer = new PoolReducer(getConnectionPool(), reducer,
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
            setStatementCache(buildStatementCache(statementCacheMaxSize));
    }

    private ConcurrentMap<StatementKey, ValueHolder<Statement>> buildStatementCache(int statementCacheMaxSize) {
        EvictionListener<StatementKey, ValueHolder<Statement>> listener =
            new EvictionListener<StatementKey, ValueHolder<Statement>>() {
                public void onEviction(StatementKey key, ValueHolder<Statement> value) {
                    closeStatement(value.value());
                }
            };
        return new ConcurrentLinkedHashMap.Builder<StatementKey, ValueHolder<Statement>>()
            .initialCapacity(statementCacheMaxSize)
            .maximumWeightedCapacity(statementCacheMaxSize)
            .listener(listener)
            .build();
    }

    private void closeStatement(Statement statement) {
        try {
            statement.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close cached statement " + statement);
        }
    }

    /**
     * Terminates this datasource. Once terminated the datasource cannot be more revived.
     */
    public synchronized void terminate() {
        if (state == State.TERMINATED) return;
        State oldState = state;
        state = State.TERMINATED;
        if (oldState == State.NEW) return;

        ConcurrentMap<StatementKey, ValueHolder<Statement>> statementCache = getStatementCache();
        if (statementCache != null)
            for (Iterator<ValueHolder<Statement>> i = statementCache.values().iterator(); i.hasNext(); ) {
                ValueHolder<Statement> valueHolder = i.next();
                closeStatement(valueHolder.value());
                i.remove();
            }
        poolReducer.terminate();
        getConnectionPool().terminate();
    }

    private void validateConfig() {
        if (getDriverClassName() == null || getJdbcUrl() == null
            || getCreateConnectionTimeoutInMs() < 0 || getAcquireRetryDelayInMs() < 0
            || getAcquireRetryAttempts() < 0 || getQueryExecuteTimeLimitInMs() < 0
            || getStatementCacheMaxSize() < 0
            || (getConnectionIdleLimitInSeconds() >= 0 && getTestConnectionQuery() == null))
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

    /** {@inheritDoc} */
    public Connection getConnection() throws SQLException {
        return getConnection(getCreateConnectionTimeoutInMs());
    }

    /** {@inheritDoc} */
    public Connection getConnection(String username, String password) throws SQLException {
        logger.warn("Using different usernames/passwords is not supported yet. Will use the defaults.");
        return getConnection();
    }

    private Connection getConnection(long timeout) throws SQLException {
        Holder<ConnState> hConnection = timeout == 0 ?
            getConnectionPool().take() : getConnectionPool().tryTake(timeout, TimeUnit.MILLISECONDS);
        if (hConnection == null)
            throw new SQLException("Couldn't obtain SQL connection.");
        return Proxy.newConnection(hConnection, this);
    }

    /** {@inheritDoc} */
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    /** {@inheritDoc} */
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    /** {@inheritDoc} */
    public void setLoginTimeout(int seconds) throws SQLException {
        setCreateConnectionTimeoutInMs(seconds * 1000);
    }

    /** {@inheritDoc} */
    public int getLoginTimeout() throws SQLException {
        return (int) getCreateConnectionTimeoutInMs() / 1000;
    }

    /** {@inheritDoc} */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("not a wrapper");
    }

    /** {@inheritDoc} */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void onDestroy(Connection connection) {
        ConcurrentMap<StatementKey, ValueHolder<Statement>> statementCache = getStatementCache();
        if (statementCache == null)
            return;

        for (StatementKey key : statementCache.keySet())
            if (key.getProxy().equals(connection)) {
                ValueHolder<Statement> valueHolder = statementCache.remove(key);
                if (valueHolder != null)
                    closeStatement(valueHolder.value());
            }
    }

    public State getState() {
        return state;
    }
}
