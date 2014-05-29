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
import org.vibur.dbcp.cache.MethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.dbcp.cache.StatementInvocationCacheProvider;
import org.vibur.dbcp.jmx.ViburDBCPMonitoring;
import org.vibur.dbcp.pool.PoolOperations;

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
import java.util.logging.Logger;

import static org.vibur.dbcp.util.SqlUtils.closeStatement;
import static org.vibur.dbcp.util.ViburUtils.NEW_LINE;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * The main DataSource which needs to be configured/instantiated by the calling application and from
 * which the JDBC Connections will be obtained via calling the {@link #getConnection()} method. The
 * lifecycle operations of this DataSource are defined by the {@link DataSourceLifecycle} interface.
 *
 * @see DataSource
 * @see org.vibur.dbcp.pool.ConnectionFactory
 *
 * @author Simeon Malchev
 */
public class ViburDBCPDataSource extends ViburDBCPConfig implements DataSource, DataSourceLifecycle {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ViburDBCPDataSource.class);

    private static final int CACHE_MAX_SIZE = 1000;

    public static final String DEFAULT_PROPERTIES_CONFIG_FILE_NAME = "vibur-dbcp-config.properties";
    public static final String DEFAULT_XML_CONFIG_FILE_NAME = "vibur-dbcp-config.xml";

    private PrintWriter logWriter = null;

    private State state = State.NEW;

    /**
     * Default constructor for programmatic configuration via the {@code ViburDBCPConfig}
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
        this();
        URL config;
        if (configFileName != null) {
            config = getURL(configFileName);
            if (config == null)
                throw new ViburDBCPException("Unable to load resource " + configFileName);
        } else {
            config = getURL(DEFAULT_XML_CONFIG_FILE_NAME);
            if (config == null) {
                config = getURL(DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
                if (config == null)
                    throw new ViburDBCPException("Unable to load default resources " + DEFAULT_XML_CONFIG_FILE_NAME
                        + " or " + DEFAULT_PROPERTIES_CONFIG_FILE_NAME);
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
     * Initialization via the given properties.
     *
     * @param properties the given properties
     * @throws ViburDBCPException if cannot configure successfully
     */
    public ViburDBCPDataSource(Properties properties) throws ViburDBCPException {
        this();
        configureFromProperties(properties);
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
            if (inputStream != null)
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                    logger.warn("Couldn't close configuration URL " + config, ignored);
                }
        }
    }

    private void configureFromProperties(Properties properties) throws ViburDBCPException {
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
                    else
                        throw new ViburDBCPException("Unexpected field found in ViburDBCPConfig: " + field.getName());
                }
            } catch (NumberFormatException e) {
                throw new ViburDBCPException(field.getName(), e);
            } catch (IllegalAccessException e) {
                throw new ViburDBCPException(field.getName(), e);
            }
        }
    }

    private void set(Field field, Object value) throws IllegalAccessException {
        field.setAccessible(true);
        field.set(this, value);
    }

    /**
     * {@inheritDoc}

     * @throws ViburDBCPException if cannot start this DataSource successfully
     */
    public synchronized void start() throws ViburDBCPException {
        if (state != State.NEW)
            throw new IllegalStateException();
        state = State.WORKING;

        validateConfig();

        setPoolOperations(new PoolOperations(this));
        initStatementCache();

        initJMX();
        logger.debug("Started {}", this);
    }

    /** {@inheritDoc} */
    public synchronized void terminate() {
        if (state == State.TERMINATED) return;
        State oldState = state;
        state = State.TERMINATED;
        if (oldState == State.NEW) return;

        ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache = getStatementCache();
        if (statementCache != null) {
            for (Iterator<ReturnVal<Statement>> i = statementCache.values().iterator(); i.hasNext(); ) {
                ReturnVal<Statement> returnVal = i.next();
                closeStatement(returnVal.value());
                i.remove();
            }
            setStatementCache(null);
        }

        getPoolOperations().terminate();
        logger.debug("Terminated {}", this);
    }

    private void validateConfig() {
        if (getExternalDataSource() == null && getJdbcUrl() == null) throw new IllegalArgumentException();
        if (getAcquireRetryDelayInMs() < 0) throw new IllegalArgumentException();
        if (getAcquireRetryAttempts() < 0) throw new IllegalArgumentException();
        if (getConnectionTimeoutInMs() < 0) throw new IllegalArgumentException();
        if (getLoginTimeoutInSeconds() < 0) throw new IllegalArgumentException();
        if (getStatementCacheMaxSize() < 0) throw new IllegalArgumentException();
        if (getReducerTimeIntervalInSeconds() < 0) throw new IllegalArgumentException();
        if (getReducerSamples() <= 0) throw new IllegalArgumentException();
        if (getConnectionIdleLimitInSeconds() >= 0 && getTestConnectionQuery() == null)
            throw new IllegalArgumentException();

        if (getPassword() == null) logger.warn("JDBC password is not specified.");
        if (getUsername() == null) logger.warn("JDBC username is not specified.");

        if (getDefaultTransactionIsolation() != null) {
            String defaultTransactionIsolation = getDefaultTransactionIsolation().toUpperCase();
            if (defaultTransactionIsolation.equals("NONE"))
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_NONE);
            else if (defaultTransactionIsolation.equals("READ_COMMITTED"))
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_READ_COMMITTED);
            else if (defaultTransactionIsolation.equals("REPEATABLE_READ"))
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_REPEATABLE_READ);
            else if (defaultTransactionIsolation.equals("READ_UNCOMMITTED"))
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_READ_UNCOMMITTED);
            else if (defaultTransactionIsolation.equals("SERIALIZABLE"))
                setDefaultTransactionIsolationValue(Connection.TRANSACTION_SERIALIZABLE);
            else
                logger.warn("Unknown defaultTransactionIsolation {}. Will use the driver's default.",
                    getDefaultTransactionIsolation());
        }
    }

    private void initStatementCache() {
        int statementCacheMaxSize = getStatementCacheMaxSize();
        if (statementCacheMaxSize > CACHE_MAX_SIZE)
            statementCacheMaxSize = CACHE_MAX_SIZE;
        if (statementCacheMaxSize > 0)
            setStatementCache(new StatementInvocationCacheProvider(statementCacheMaxSize).build());
    }

    private void initJMX() throws ViburDBCPException {
        if (isEnableJMX())
            new ViburDBCPMonitoring(this);
    }

    /** {@inheritDoc} */
    public Connection getConnection() throws SQLException {
        return getConnection(getConnectionTimeoutInMs());
    }

    /** {@inheritDoc} */
    public Connection getConnection(String username, String password) throws SQLException {
        logger.warn("Different usernames/passwords are not supported yet. Will use the configured defaults.");
        return getConnection();
    }

    /**
     * Mainly exists to provide getConnection() method timing logging.
     */
    private Connection getConnection(long timeout) throws SQLException {
        boolean shouldLog = getLogConnectionLongerThanMs() >= 0;
        long startTime = shouldLog ? System.currentTimeMillis() : 0L;

        try {
            return getPoolOperations().getConnection(timeout);
        } finally {
            if (shouldLog)
                logGetConnection(timeout, startTime);
        }
    }

    private void logGetConnection(long timeout, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= getLogConnectionLongerThanMs()) {
            StringBuilder log = new StringBuilder(String.format("Call to getConnection(%d) took %d ms.",
                timeout, timeTaken));
            if (isLogStackTraceForLongConnection())
                log.append(NEW_LINE).append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(log.toString());
        }
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
        setLoginTimeoutInSeconds(seconds);
    }

    /** {@inheritDoc} */
    public int getLoginTimeout() throws SQLException {
        return getLoginTimeoutInSeconds();
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
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    /** {@inheritDoc} */
    public State getState() {
        return state;
    }
}
