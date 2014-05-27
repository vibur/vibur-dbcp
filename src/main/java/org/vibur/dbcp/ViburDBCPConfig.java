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

package org.vibur.dbcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.cache.MethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.dbcp.pool.PoolOperations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Specifies all {@link ViburDBCPDataSource} configuration options.
 *
 * @author Simeon Malchev
 * @author Daniel Caldeweyher
 */
public class ViburDBCPConfig {

    private static final Logger logger = LoggerFactory.getLogger(ViburDBCPConfig.class);

    /** Database driver class name. This is <b>an optional</b> parameter if the driver is JDBC 4 complaint. If specified,
     * a call to {@code Class.forName(driverClassName).newInstance()} will be issued during the Vibur DBCP initialization.
     * This is needed when Vibur DBCP is used in an OSGi container and may also be helpful if Vibur DBCP is used in an
     * Apache Tomcat web application which has its JDBC driver JAR file packaged in the app WEB-INF/lib directory.
     * If this property is not specified, then Vibur DBCP will depend on the JavaSE Service Provider mechanism to find
     * the driver. */
    private String driverClassName = null;
    /** Database JDBC Connection string. */
    private String jdbcUrl;
    /** User name to use. */
    private String username;
    /** Password to use. */
    private String password;
    /** If specified, this {@code externalDataSource} will be used as an alternative way to obtain the raw
     * connections for the pool instead of calling {@code DriverManager.getConnection()}. */
    private DataSource externalDataSource = null;


    /** If the connection has stayed in the pool for at least {@code connectionIdleLimitInSeconds},
     * it will be validated before being given to the application using the {@code testConnectionQuery}.
     * If set to {@code 0}, will validate the connection always when it is taken from the pool.
     * If set to a negative number, will never validate the taken from the pool connection. */
    private int connectionIdleLimitInSeconds = 60;

    public static final int QUERY_TIMEOUT = 5; // in seconds
    public static final String IS_VALID_QUERY = "isValid";

    /** Used to test the validity of a JDBC Connection. If the {@code connectionIdleLimitInSeconds} is set to
     * a non-negative number, the {@code testConnectionQuery} should be set to a valid SQL query, for example
     * {@code SELECT 1}, or to {@code isValid} in which case the {@code Connection.isValid()} method will be used.
     *
     * <p>Similarly to the spec for {@link java.sql.Connection#isValid(int)}, if a custom {@code testConnectionQuery}
     * is specified, it will be executed in the context of the current transaction. */
    private String testConnectionQuery = IS_VALID_QUERY;

    /** An SQL query which will be run only once when a JDBC Connection is first created. This property should be
     * set to a valid SQL query, to {@code null} which means no query, or to {@code isValid} which means that the
     * {@code Connection.isValid()} method will be used. An use case in which this property can be useful is when the
     * application is connecting to the database via some middleware, for example, connecting to PostgreSQL server(s)
     * via PgBouncer. */
    private String initSQL = null;


    /** The pool initial size, i.e. the initial number of JDBC Connections allocated in this pool. */
    private int poolInitialSize = 10;
    /** The pool max size, i.e. the maximum number of JDBC Connections allocated in this pool. */
    private int poolMaxSize = 100;
    /** If `true`, guarantees that the threads invoking the pool's {@code take} methods will be selected to obtain a
     * connection from it in FIFO order, and no thread will be starved out from accessing the pool's underlying
     * resources. */
    private boolean poolFair = true;
    /** If {@code true}, the pool will keep information for the current stack trace of every taken connection. */
    private boolean poolEnableConnectionTracking = false;


    private static final AtomicInteger idGenerator = new AtomicInteger(1);
    private static final ConcurrentMap<String, Boolean> names = new ConcurrentHashMap<String, Boolean>();
    /** The DataSource name, mostly useful for JMX identification and similar. This {@code name} must be unique
     * among all names for all configured DataSources. The default name is an auto generated integer id. If the
     * configured {@code name} is not unique then the default auto generated id will be used instead. */
    private String name = Integer.toString(idGenerator.getAndIncrement());

    /** Enables or disables the DataSource JMX exposure. */
    private boolean enableJMX = true;


    /** For more details on the next 2 parameters see {@link org.vibur.objectpool.util.SamplingPoolReducer}.
     */
    /** The time period after which the {@code poolReducer} will try to possibly reduce the number of created
     * but unused JDBC Connections in this pool. {@code 0} disables it. */
    private int reducerTimeIntervalInSeconds = 60;
    /** How many times the {@code poolReducer} will wake up during the given
     * {@code reducerTimeIntervalInSeconds} period in order to sample various information from this pool. */
    private int reducerSamples = 20;


    /** Time to wait before a call to {@code getConnection()} times out and returns an error, for the case when
     * there is an available and valid connection in the pool. {@code 0} means forever.
     *
     * <p> If there is not an available and valid connection in the pool, the total maximum time which the
     * call to {@code getConnection()} may take before it times out and returns an error can be calculated as: <br>
     * maxTimeoutInMs = connectionTimeoutInMs
     *     + (acquireRetryAttempts + 1) * loginTimeoutInSeconds * 1000
     *     + acquireRetryAttempts * acquireRetryDelayInMs */
    private long connectionTimeoutInMs = 30000;
    /** Login timeout which is to be set to the call to {@code DriverManager.setLoginTimeout()}
     * or {@code getExternalDataSource().setLoginTimeout()} during the initialization process of the DataSource. */
    private int loginTimeoutInSeconds = 10;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * wait for this long time before attempting to acquire a new JDBC Connection again. */
    private long acquireRetryDelayInMs = 1000;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * try to connect these many times before giving up. */
    private int acquireRetryAttempts = 3;


    /** Defines the maximum statement cache size. {@code 0} disables it, max values is {@code 1000}.
     * If the statement's cache is not enabled, the client application may safely exclude the dependency
     * on ConcurrentLinkedCacheMap from its pom.xml file. */
    private int statementCacheMaxSize = 0;
    private ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache = null;

    /** The list of critical SQL states as a comma separated values, see http://stackoverflow.com/a/14412929/1682918 .
     * If an SQL exception which has any of these SQL states is thrown then all connections in the pool will be
     * considered invalid and will be closed. */
    private String criticalSQLStates = "08001,08006,08007,08S01,57P01,57P02,57P03,JZ0C0,JZ0C1";
    private PoolOperations poolOperations;


    /** {@code getConnection} method calls taking longer than or equal to this time limit are logged at WARN level.
     * A value of {@code 0} will log all such calls. A {@code negative number} disables it. */
    private long logConnectionLongerThanMs = 3000;
    /** Will apply only if {@link #logConnectionLongerThanMs} is enabled, and if set to {@code true},
     * will log at WARN level the current {@code getConnection} call stack trace plus the time taken. */
    private boolean logStackTraceForLongConnection = false;
    /** JDBC Statement {@code execute...} calls taking longer than or equal to this time limit are logged at
     * WARN level. A value of {@code 0} will log all such calls. A {@code negative number} disables it.
     *
     * <p><b>Note that</b> while a JDBC Statement {@code execute...} call duration is roughly equivalent to
     * the execution time of the underlying SQL query, the overall call duration may also include some Java GC
     * time, JDBC driver specific execution time, and threads context switching time (the last could be significant
     * if the application has a very large thread count). */
    private long logQueryExecutionLongerThanMs = 3000;
    /** Will apply only if {@link #logQueryExecutionLongerThanMs} is enabled, and if set to {@code true},
     * will log at WARN level the current JDBC Statement {@code execute...} call stack trace plus the
     * underlying SQL query and the time taken. */
    private boolean logStackTraceForLongQueryExecution = false;


    /** If set to {@code true}, will reset the connection default values below, always after the
     * connection is restored (returned) to the pool after use. If the calling application never changes
     * these default values, resetting them is not needed. */
    private boolean resetDefaultsAfterUse = false;
    /** The default auto-commit state of the created connections. */
    private Boolean defaultAutoCommit;
    /** The default read-only state of the created connections. */
    private Boolean defaultReadOnly;
    /** The default transaction isolation state of the created connections. */
    private String defaultTransactionIsolation;
    /** The default catalog state of the created connections. */
    private String defaultCatalog;
    /** The parsed transaction isolation value. Default = driver value. */
    private Integer defaultTransactionIsolationValue;


    //////////////////////// Getters & Setters ////////////////////////

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DataSource getExternalDataSource() {
        return externalDataSource;
    }

    public void setExternalDataSource(DataSource externalDataSource) {
        this.externalDataSource = externalDataSource;
    }

    public int getConnectionIdleLimitInSeconds() {
        return connectionIdleLimitInSeconds;
    }

    public void setConnectionIdleLimitInSeconds(int connectionIdleLimitInSeconds) {
        this.connectionIdleLimitInSeconds = connectionIdleLimitInSeconds;
    }

    public String getTestConnectionQuery() {
        return testConnectionQuery;
    }

    public void setTestConnectionQuery(String testConnectionQuery) {
        this.testConnectionQuery = testConnectionQuery;
    }

    public String getInitSQL() {
        return initSQL;
    }

    public void setInitSQL(String initSQL) {
        this.initSQL = initSQL;
    }

    public int getPoolInitialSize() {
        return poolInitialSize;
    }

    public void setPoolInitialSize(int poolInitialSize) {
        this.poolInitialSize = poolInitialSize;
    }

    public int getPoolMaxSize() {
        return poolMaxSize;
    }

    public void setPoolMaxSize(int poolMaxSize) {
        this.poolMaxSize = poolMaxSize;
    }

    public boolean isPoolFair() {
        return poolFair;
    }

    public void setPoolFair(boolean poolFair) {
        this.poolFair = poolFair;
    }

    public boolean isPoolEnableConnectionTracking() {
        return poolEnableConnectionTracking;
    }

    public void setPoolEnableConnectionTracking(boolean poolEnableConnectionTracking) {
        this.poolEnableConnectionTracking = poolEnableConnectionTracking;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (names.putIfAbsent(name, Boolean.TRUE) == null)
            this.name = name;
        else
            logger.warn("DataSource name {} is not unique, using {} instead", name, this.name);
    }

    public boolean isEnableJMX() {
        return enableJMX;
    }

    public void setEnableJMX(boolean enableJMX) {
        this.enableJMX = enableJMX;
    }

    public int getReducerTimeIntervalInSeconds() {
        return reducerTimeIntervalInSeconds;
    }

    public void setReducerTimeIntervalInSeconds(int reducerTimeIntervalInSeconds) {
        this.reducerTimeIntervalInSeconds = reducerTimeIntervalInSeconds;
    }

    public int getReducerSamples() {
        return reducerSamples;
    }

    public void setReducerSamples(int reducerSamples) {
        this.reducerSamples = reducerSamples;
    }

    public long getConnectionTimeoutInMs() {
        return connectionTimeoutInMs;
    }

    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
    }

    public int getLoginTimeoutInSeconds() {
        return loginTimeoutInSeconds;
    }

    public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
        this.loginTimeoutInSeconds = loginTimeoutInSeconds;
    }

    public long getAcquireRetryDelayInMs() {
        return acquireRetryDelayInMs;
    }

    public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
        this.acquireRetryDelayInMs = acquireRetryDelayInMs;
    }

    public int getAcquireRetryAttempts() {
        return acquireRetryAttempts;
    }

    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        this.acquireRetryAttempts = acquireRetryAttempts;
    }

    public int getStatementCacheMaxSize() {
        return statementCacheMaxSize;
    }

    public void setStatementCacheMaxSize(int statementCacheMaxSize) {
        this.statementCacheMaxSize = statementCacheMaxSize;
    }

    public ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> getStatementCache() {
        return statementCache;
    }

    public void setStatementCache(ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache) {
        this.statementCache = statementCache;
    }

    public String getCriticalSQLStates() {
        return criticalSQLStates;
    }

    public void setCriticalSQLStates(String criticalSQLStates) {
        this.criticalSQLStates = criticalSQLStates;
    }

    public PoolOperations getPoolOperations() {
        return poolOperations;
    }

    public void setPoolOperations(PoolOperations poolOperations) {
        this.poolOperations = poolOperations;
    }

    public long getLogConnectionLongerThanMs() {
        return logConnectionLongerThanMs;
    }

    public void setLogConnectionLongerThanMs(long logConnectionLongerThanMs) {
        this.logConnectionLongerThanMs = logConnectionLongerThanMs;
    }

    public boolean isLogStackTraceForLongConnection() {
        return logStackTraceForLongConnection;
    }

    public void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection) {
        this.logStackTraceForLongConnection = logStackTraceForLongConnection;
    }

    public long getLogQueryExecutionLongerThanMs() {
        return logQueryExecutionLongerThanMs;
    }

    public void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs) {
        this.logQueryExecutionLongerThanMs = logQueryExecutionLongerThanMs;
    }

    public boolean isLogStackTraceForLongQueryExecution() {
        return logStackTraceForLongQueryExecution;
    }

    public void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution) {
        this.logStackTraceForLongQueryExecution = logStackTraceForLongQueryExecution;
    }

    public boolean isResetDefaultsAfterUse() {
        return resetDefaultsAfterUse;
    }

    public void setResetDefaultsAfterUse(boolean resetDefaultsAfterUse) {
        this.resetDefaultsAfterUse = resetDefaultsAfterUse;
    }

    public Boolean getDefaultAutoCommit() {
        return defaultAutoCommit;
    }

    public void setDefaultAutoCommit(Boolean defaultAutoCommit) {
        this.defaultAutoCommit = defaultAutoCommit;
    }

    public Boolean getDefaultReadOnly() {
        return defaultReadOnly;
    }

    public void setDefaultReadOnly(Boolean defaultReadOnly) {
        this.defaultReadOnly = defaultReadOnly;
    }

    public String getDefaultTransactionIsolation() {
        return defaultTransactionIsolation;
    }

    public void setDefaultTransactionIsolation(String defaultTransactionIsolation) {
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public void setDefaultCatalog(String defaultCatalog) {
        this.defaultCatalog = defaultCatalog;
    }

    public Integer getDefaultTransactionIsolationValue() {
        return defaultTransactionIsolationValue;
    }

    public void setDefaultTransactionIsolationValue(Integer defaultTransactionIsolationValue) {
        this.defaultTransactionIsolationValue = defaultTransactionIsolationValue;
    }

    public String toString() {
        return getClass().getSimpleName() +
            " {driverClassName='" + driverClassName + '\'' +
            ", jdbcUrl='" + jdbcUrl + '\'' +
            ", externalDataSource=" + externalDataSource +
            ", poolInitialSize=" + poolInitialSize +
            ", poolMaxSize=" + poolMaxSize +
            ", poolFair=" + poolFair +
            ", name='" + name + '\'' +
            ", statementCacheMaxSize=" + statementCacheMaxSize +
            '}';
    }
}
