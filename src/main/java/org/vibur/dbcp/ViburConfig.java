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
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.event.*;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.PoolReducer;
import org.vibur.dbcp.pool.ViburObjectFactory;
import org.vibur.dbcp.util.JdbcUtils;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.TakenListener;
import org.vibur.objectpool.util.ThreadedPoolReducer;

import javax.sql.DataSource;
import java.sql.Driver;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * Specifies all {@link ViburDBCPDataSource} configuration options.
 *
 * @author Simeon Malchev
 * @author Daniel Caldeweyher
 */
public abstract class ViburConfig {

    private static final Logger logger = LoggerFactory.getLogger(ViburConfig.class);

    public static final String DEFAULT_PROPERTIES_CONFIG_FILE_NAME = "vibur-dbcp-config.properties";
    public static final String DEFAULT_XML_CONFIG_FILE_NAME = "vibur-dbcp-config.xml";

    public static final String SQLSTATE_POOL_NOTSTARTED_ERROR = "VI000";
    public static final String SQLSTATE_POOL_CLOSED_ERROR     = "VI001";
    public static final String SQLSTATE_TIMEOUT_ERROR         = "VI002";
    public static final String SQLSTATE_CONN_INIT_ERROR       = "VI003";
    public static final String SQLSTATE_OBJECT_CLOSED_ERROR   = "VI004";
    public static final String SQLSTATE_WRAPPER_ERROR         = "VI005";

    static final int STATEMENT_CACHE_MAX_SIZE = 2000;

    ViburConfig() { }

    /** The user name to use when connecting to the database. */
    private String username;
    /** The password to use when connecting to the database. */
    private String password;

    /** The preferred way to configure/inject the JDBC Driver through which the Connections will be generated. */
    private Driver driver = null;
    /** The driver properties that will be used in the call to {@link java.sql.Driver#connect}. */
    private Properties driverProperties = null;
    /** The database driver class name. This is <b>an optional</b> parameter if the {@link #driver} parameter above is
     * specified of if the driver is JDBC 4 complaint. If configured, a call to
     * {@code Class.forName(driverClassName).newInstance()} will be issued during the Vibur DBCP initialization.
     * This is needed when Vibur DBCP is used in an OSGi container and may also be helpful if Vibur DBCP is used in an
     * Apache Tomcat web application which has its JDBC driver JAR file packaged in the app WEB-INF/lib directory.
     * If this property is not specified, then Vibur DBCP will depend on the JavaSE Service Provider mechanism to find
     * the driver. */
    private String driverClassName = null;
    /** The database JDBC Connection string. */
    private String jdbcUrl;

    /** If specified, this {@code externalDataSource} will be used as an alternative way to obtain the raw
     * connections for the pool instead of relaying on {@link java.sql.Driver}. */
    private DataSource externalDataSource = null;

    private JdbcUtils.Connector connector = null;


    /** If the connection has stayed in the pool for at least {@code connectionIdleLimitInSeconds},
     * it will be validated before being given to the application using the {@link #testConnectionQuery}.
     * If set to {@code 0}, will validate the connection always when it is taken from the pool.
     * If set to a negative number, will never validate the taken from the pool connection. */
    private int connectionIdleLimitInSeconds = 5;
    /** The timeout that will be passed to the call to {@link #testConnectionQuery} when a taken
     * from the pool JDBC Connection is validated before use, or when {@link #initSQL} is executed (if specified).
     * {@code 0} means no limit. */
    private int validateTimeoutInSeconds = 3;

    public static final String IS_VALID_QUERY = "isValid";

    /** Used to test the validity of a JDBC Connection. If the {@link #connectionIdleLimitInSeconds} is set to
     * a non-negative number, the {@code testConnectionQuery} should be set to a valid SQL query, for example
     * {@code SELECT 1}, or to {@code isValid} in which case the {@link java.sql.Connection#isValid} method
     * will be used.
     *
     * <p>Similarly to the spec for {@link java.sql.Connection#isValid}, if a custom {@code testConnectionQuery}
     * is specified, it will be executed in the context of the current transaction.
     *
     * <p>Note that if the driver is JDBC 4 compliant, using the default {@code isValid} value is
     * <b>strongly recommended</b>, as the driver can often use some ad-hoc and very efficient mechanism via which
     * to positively verify whether the given JDBC connection is still valid or not. */
    private String testConnectionQuery = IS_VALID_QUERY;

    /** An SQL query which will be run only once when a JDBC Connection is first created. This property should be
     * set to a valid SQL query, to {@code null} which means no query, or to {@code isValid} which means that the
     * {@link java.sql.Connection#isValid} method will be used. An use case in which this property can be useful
     * is when the application is connecting to the database via some middleware, for example, connecting to PostgreSQL
     * server(s) via PgBouncer. */
    private String initSQL = null;

    /** This option applies only if {@link #testConnectionQuery} or {@link #initSQL} are enabled and if at least one
     * of them has a value different than {@code IS_VALID_QUERY} ({@code isValid}). If enabled,
     * the calls to the validation or initialization SQL query will be preceded by a call to
     * {@link java.sql.Connection#setNetworkTimeout}, and after that the original network
     * timeout value will be restored.
     *
     * <p>Note that it is responsibility of the application developer to make sure that the used by the application
     * JDBC driver supports {@code setNetworkTimeout}. */
    private boolean useNetworkTimeout = false;
    /** This option applies only if {@code useNetworkTimeout} is enabled. This is the {@code Executor} that will
     * be passed to the call of {@link java.sql.Connection#setNetworkTimeout}.
     *
     * <p>Note that it is responsibility of the application developer to supply {@link Executor} that is suitable
     * for the needs of the application JDBC driver. For example, some JDBC drivers may require a synchronous
     * {@code Executor}. */
    private Executor networkTimeoutExecutor = null;


    /** The pool initial size, i.e. the initial number of JDBC Connections allocated in this pool. */
    private int poolInitialSize = 10;
    /** The pool max size, i.e. the maximum number of JDBC Connections allocated in this pool. */
    private int poolMaxSize = 100;
    /** If `true`, guarantees that the threads invoking the pool's {@link org.vibur.objectpool.PoolService#take}
     * methods will be selected to obtain a connection from it in FIFO order, and no thread will be starved out from
     * accessing the pool's underlying resources. */
    private boolean poolFair = true;
    /** If {@code true}, the pool will keep information for the current stack trace of every taken connection.
     * See also {@link #logTakenConnectionsOnTimeout}. */
    private boolean poolEnableConnectionTracking = false;

    private boolean poolFifo = false;

    private PoolService<ConnHolder> pool = null;
    private ViburObjectFactory connectionFactory = null;
    private ThreadedPoolReducer poolReducer = null;

    /** In rare circumstances, the application may need to obtain a non-pooled connection from the pool
     * after the pool has been terminated. This may happen as part of some post-caching or application
     * shutdown execution path. */
    private boolean allowConnectionAfterTermination = false;

    /** Controls whether the pool's {@code DataSource} and the created from it JDBC objects ({@code Connection},
     * {@code Statement}, etc) support unwrapping/exposing of the underlying (proxied) JDBC objects. If disabled,
     * the call to {@link java.sql.Wrapper#isWrapperFor} on any of these objects will always return {@code false}.
     */
    private boolean allowUnwrapping = true;


    private static final AtomicInteger idGenerator = new AtomicInteger(1);
    private final String defaultName = "p" + Integer.toString(idGenerator.getAndIncrement());

    /** The DataSource/pool name, used for JMX identification, logging and similar. If is responsibility of the
     * application to set an unique name to each pool if it uses more than one pool. The default name is "p" + an auto
     * generated integer id. The {@code name} can be set only once; pool renaming is not supported. */
    private String name = defaultName;

    /** Enables or disables the DataSource JMX exposure. */
    private boolean enableJMX = true;


    /** The fully qualified pool reducer class name. This pool reducer class will be instantiated via reflection,
     * and will be instantiated only if {@link #reducerTimeIntervalInSeconds} is greater than {@code 0}.
     * It must implements the {@link org.vibur.objectpool.util.ThreadedPoolReducer} interface and must also have
     * a public constructor accepting a single argument of type {@code ViburConfig}. */
    private String poolReducerClass = PoolReducer.class.getName();

    /** For more details on the next 2 parameters see {@link org.vibur.objectpool.util.SamplingPoolReducer}.
     */
    /** The time period after which the {@code poolReducer} will try to possibly reduce the number of created
     * but unused JDBC Connections in this pool. {@code 0} disables it. */
    private int reducerTimeIntervalInSeconds = 60;
    /** How many times the {@code poolReducer} will wake up during the given
     * {@link #reducerTimeIntervalInSeconds} period in order to sample various information from this pool. */
    private int reducerSamples = 20;


    /** The time to wait before a call to {@code getConnection()} times out and throws an {@code SQLException},
     * for the case when there is an available and valid connection in the pool. {@code 0} means forever.
     *
     * <p>If there is not an available and valid connection in the pool, and if the maximum number of allowed
     * connections is not yet reached, the total maximum time that the call to {@code getConnection()} can take
     * before it times out and throws an {@code SQLException} is defined as:
     * <pre>
     * maxTimeoutInMs = connectionTimeoutInMs
     *     + (acquireRetryAttempts + 1) * loginTimeoutInSeconds * 1000
     *     + acquireRetryAttempts * acquireRetryDelayInMs
     * </pre> */
    private long connectionTimeoutInMs = 30000;
    /** The login timeout that will be set to the call to {@code DriverManager.setLoginTimeout()}
     * or {@code getExternalDataSource().setLoginTimeout()} during the initialization process of the DataSource. */
    private int loginTimeoutInSeconds = 10;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * wait for this long before attempting to acquire a new JDBC Connection again. */
    private long acquireRetryDelayInMs = 1000;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * try to connect these many times before giving up. */
    private int acquireRetryAttempts = 3;


    /** Defines the maximum statement cache size. {@code 0} disables it, max values is {@link #STATEMENT_CACHE_MAX_SIZE}.
     * If the statement's cache is not enabled, the client application may safely exclude the dependency
     * on ConcurrentLinkedCacheMap from its pom.xml file. */
    private int statementCacheMaxSize = 0;
    private StatementCache statementCache = null;


    /** The list of critical SQL states as a comma separated values, see http://stackoverflow.com/a/14412929/1682918 .
     * If an SQL exception that has any of these SQL states occurs then all connections in the pool will be
     * considered invalid and will be closed. */
    private String criticalSQLStates = "08001,08006,08007,08S01,57P01,57P02,57P03,JZ0C0,JZ0C1";


    /** {@code getConnection} method calls taking longer than or equal to this time limit are logged at WARN level.
     * A value of {@code 0} will log all such calls. A {@code negative number} disables it.
     *
     * <p>If the value of {@code logConnectionLongerThanMs} is greater than {@code connectionTimeoutInMs},
     * then {@code logConnectionLongerThanMs} will be set to the value of {@code connectionTimeoutInMs}. */
    private long logConnectionLongerThanMs = 3000;
    /** Will apply only if {@link #logConnectionLongerThanMs} is enabled, and if set to {@code true},
     * will log at WARN level the current {@code getConnection} call stack trace. */
    private boolean logStackTraceForLongConnection = false;
    /** The underlying SQL queries (including their concrete parameters if {@link #includeQueryParameters} is set to
     * {@code true}) from a JDBC Statement {@code execute...} calls taking longer than or equal to this time limit
     * are logged at WARN level. A value of {@code 0} will log all such calls. A {@code negative number} disables it.
     *
     * <p><b>Note that</b> while a JDBC Statement {@code execute...} call duration is roughly equivalent to
     * the execution time of the underlying SQL query, the overall call duration may also include some Java GC
     * time, JDBC driver specific execution time, and threads context switching time (the last could be
     * significant if the application has a very large thread count). */
    private long logQueryExecutionLongerThanMs = 3000;
    /** Will apply only if {@link #logQueryExecutionLongerThanMs} is enabled, and if set to {@code true},
     * will log at WARN level the current JDBC Statement {@code execute...} call stack trace. */
    private boolean logStackTraceForLongQueryExecution = false;
    /** The underlying SQL queries (including their concrete parameters if {@link #includeQueryParameters} is set to
     * {@code true}) from a JDBC Statement {@code execute...} calls that generate ResultSets with length greater than
     * or equal to this limit are logged at WARN level. A {@code negative number} disables it. Retrieving of a large
     * ResultSet may have negative effect on the application performance and may sometimes be an indication of a very
     * subtle application bug, where the whole ResultSet is retrieved, but only the first few records of it are
     * subsequently read and processed.
     *
     * <p>The logging is done at the moment when the application issues a call to the {@code ResultSet.close()}
     * method. Applications that rely on the <i>implicit</i> closure of the {@code ResultSet} when the generated it
     * {@code Statement} is closed, will not be able to benefit from this logging functionality.
     *
     * <p>The calculation of the {@code ResultSet} size is done based on the number of calls that the application
     * has issued to the {@code ResultSet.next()} method. In most of the cases this is a very accurate and
     * non-intrusive method to calculate the ResultSet size, particularly in the case of a Hibernate
     * or Spring Framework JDBC application. However, this calculation mechanism may give inaccurate results
     * for some advanced applications that navigate through the {@code ResultSet} using methods such as
     * {@code first()}, {@code last()}, or {@code afterLast()}. */
    private long logLargeResultSet = 500;
    /** Will apply only if {@link #logLargeResultSet} is enabled, and if set to {@code true},
     * will log at WARN level the current {@code ResultSet.close()} call stack trace. */
    private boolean logStackTraceForLargeResultSet = false;

    /** Enables or disables inclusion of the concrete SQL query parameters for {@link #logStackTraceForLongConnection}
     * and {@link #logLargeResultSet}. Disabling the parameters inclusion can be useful if there are particular
     * compliance requirements or if the parameters are already included in the logged messages as part of the
     * {@code toString()} implementation of the {@code PreparedStatements}; the last is a JDBC driver specific feature.
     */
    private boolean includeQueryParameters = true;

    /** If set to {@code true}, and if the {@link #connectionTimeoutInMs} is reached and the call to
     * {@code getConnection()} fails with throwing an {@code SQLException}, will log at WARN level information
     * about all currently taken connections, including the stack traces of the threads that have taken them, plus
     * the threads names and states.
     *
     * <p>This options implies that the {@link #poolEnableConnectionTracking} option is enabled, and if the last
     * is not explicitly enabled it will be implicitly enabled as part of the processing of this option.
     *
     * <p><b>Note that</b> this option should be used for troubleshooting purposes only, as it may generate a very
     * large log output. The exact format of the logged message is controlled by {@link #takenConnectionsToString()}.
     */
    private boolean logTakenConnectionsOnTimeout = false;


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
    /** The default catalog of the created connections. */
    private String defaultCatalog;
    /** The parsed transaction isolation value. {@code null} means use the default driver value. */
    private Integer defaultTransactionIsolationValue;


    /** If set to {@code true}, will clear the SQL Warnings (if any) from the JDBC Connection before returning it to
     * the pool. Similarly, if statement caching is enabled, will clear the SQL Warnings from the JDBC Prepared or
     * Callable Statement before returning it to the statement cache. */
    private boolean clearSQLWarnings = false;


    /** A programming {@linkplain ConnectionHook#on hook} that will be invoked only once when
     * the raw JDBC Connection is first created. Its execution should take as short time as possible. */
    private ConnectionHook initConnectionHook = null;
    /** A programming {@linkplain ConnectionHook#on hook} that will be invoked on the raw JDBC
     * Connection as part of the {@link DataSource#getConnection()} flow. Its execution should take as short time as
     * possible. */
    private ConnectionHook connectionHook = null;
    /** A programming {@linkplain ConnectionHook#on hook} that will be invoked on the raw JDBC
     * Connection as part of the {@link java.sql.Connection#close()} flow. Its execution should take as short time as
     * possible. */
    private ConnectionHook closeConnectionHook = null;


    /** A programming {@linkplain InvocationHook#invoke hook} intercepting (almost) all method calls on all proxied
     * JDBC interfaces. Methods inherited from the {@link Object} class, methods related to the "closed" state of
     * the JDBC objects (e.g., close(), isClosed()), as well as methods from the {@link java.sql.Wrapper} interface
     * are not intercepted. The hook execution should take as short time as possible. */
    private InvocationHook invocationHook = null;


    /** Provides access to the functionality for logging of long lasting getConnection() calls, slow SQL queries,
     * and large ResultSets. Setting this parameter to a sub-class of {@link BaseViburLogger} will allow the
     * application to intercept all such logging events, and to accumulate statistics of the count and execution time
     * of the SQL queries and similar. */
    private ViburLogger viburLogger = new BaseViburLogger();
    /** Allows the application to receiving notifications for all exceptions thrown by the operations
     * invoked on a JDBC Connection object or any of its direct or indirect derivative objects, such as Statement,
     * ResultSet, or database Metadata objects. */
    private ExceptionListener exceptionListener = null;


    //////////////////////// Getters & Setters ////////////////////////

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

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }

    public Properties getDriverProperties() {
        return driverProperties;
    }

    public void setDriverProperties(Properties driverProperties) {
        this.driverProperties = driverProperties;
    }

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

    public DataSource getExternalDataSource() {
        return externalDataSource;
    }

    public void setExternalDataSource(DataSource externalDataSource) {
        this.externalDataSource = externalDataSource;
    }

    public JdbcUtils.Connector getConnector() {
        return connector;
    }

    public void setConnector(JdbcUtils.Connector connector) {
        this.connector = connector;
    }

    public int getConnectionIdleLimitInSeconds() {
        return connectionIdleLimitInSeconds;
    }

    public void setConnectionIdleLimitInSeconds(int connectionIdleLimitInSeconds) {
        this.connectionIdleLimitInSeconds = connectionIdleLimitInSeconds;
    }

    public int getValidateTimeoutInSeconds() {
        return validateTimeoutInSeconds;
    }

    public void setValidateTimeoutInSeconds(int validateTimeoutInSeconds) {
        this.validateTimeoutInSeconds = validateTimeoutInSeconds;
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

    public boolean isUseNetworkTimeout() {
        return useNetworkTimeout;
    }

    public void setUseNetworkTimeout(boolean useNetworkTimeout) {
        this.useNetworkTimeout = useNetworkTimeout;
    }

    public Executor getNetworkTimeoutExecutor() {
        return networkTimeoutExecutor;
    }

    public void setNetworkTimeoutExecutor(Executor networkTimeoutExecutor) {
        this.networkTimeoutExecutor = networkTimeoutExecutor;
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

    public boolean isPoolFifo() {
        return poolFifo;
    }

    public void setPoolFifo(boolean poolFifo) {
        this.poolFifo = poolFifo;
    }

    public PoolService<ConnHolder>  getPool() {
        return pool;
    }

    public void setPool(PoolService<ConnHolder> pool) {
        this.pool = pool;
    }

    public ViburObjectFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ViburObjectFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ThreadedPoolReducer getPoolReducer() {
        return poolReducer;
    }

    public void setPoolReducer(ThreadedPoolReducer poolReducer) {
        this.poolReducer = poolReducer;
    }

    public boolean isAllowConnectionAfterTermination() {
        return allowConnectionAfterTermination;
    }

    public void setAllowConnectionAfterTermination(boolean allowConnectionAfterTermination) {
        this.allowConnectionAfterTermination = allowConnectionAfterTermination;
    }

    public boolean isAllowUnwrapping() {
        return allowUnwrapping;
    }

    public void setAllowUnwrapping(boolean allowUnwrapping) {
        this.allowUnwrapping = allowUnwrapping;
    }

    public String getName() {
        return name;
    }

    /**
     * NOTE: the pool name can be set only once; pool renaming is not supported.
     *
     * @param name the pool name to use
     */
    public void setName(String name) {
        if (name == null || (name = name.trim()).length() == 0) {
            logger.error("Invalid pool name {}", name);
            return;
        }
        if (!defaultName.equals(this.name) || defaultName.equals(name)) {
            logger.error("Pool name is already set or duplicated, existing name = {}, incoming name = {}", this.name, name);
            return;
        }
        this.name = name;
    }

    public String getJmxName() {
        return "org.vibur.dbcp:type=ViburDBCP-" + name;
    }

    public boolean isEnableJMX() {
        return enableJMX;
    }

    public void setEnableJMX(boolean enableJMX) {
        this.enableJMX = enableJMX;
    }

    public String getPoolReducerClass() {
        return poolReducerClass;
    }

    public void setPoolReducerClass(String poolReducerClass) {
        this.poolReducerClass = poolReducerClass;
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

    public StatementCache getStatementCache() {
        return statementCache;
    }

    public void setStatementCache(StatementCache statementCache) {
        this.statementCache = statementCache;
    }

    public String getCriticalSQLStates() {
        return criticalSQLStates;
    }

    public void setCriticalSQLStates(String criticalSQLStates) {
        this.criticalSQLStates = criticalSQLStates;
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

    public long getLogLargeResultSet() {
        return logLargeResultSet;
    }

    public void setLogLargeResultSet(long logLargeResultSet) {
        this.logLargeResultSet = logLargeResultSet;
    }

    public boolean isLogStackTraceForLargeResultSet() {
        return logStackTraceForLargeResultSet;
    }

    public void setLogStackTraceForLargeResultSet(boolean logStackTraceForLargeResultSet) {
        this.logStackTraceForLargeResultSet = logStackTraceForLargeResultSet;
    }

    public boolean isIncludeQueryParameters() {
        return includeQueryParameters;
    }

    public void setIncludeQueryParameters(boolean includeQueryParameters) {
        this.includeQueryParameters = includeQueryParameters;
    }

    public boolean isLogTakenConnectionsOnTimeout() {
        return logTakenConnectionsOnTimeout;
    }

    public void setLogTakenConnectionsOnTimeout(boolean logTakenConnectionsOnTimeout) {
        this.logTakenConnectionsOnTimeout = logTakenConnectionsOnTimeout;
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

    public boolean isClearSQLWarnings() {
        return clearSQLWarnings;
    }

    public void setClearSQLWarnings(boolean clearSQLWarnings) {
        this.clearSQLWarnings = clearSQLWarnings;
    }

    public ConnectionHook getInitConnectionHook() {
        return initConnectionHook;
    }

    public void setInitConnectionHook(ConnectionHook initConnectionHook) {
        this.initConnectionHook = initConnectionHook;
    }

    public ConnectionHook getConnectionHook() {
        return connectionHook;
    }

    public void setConnectionHook(ConnectionHook connectionHook) {
        this.connectionHook = connectionHook;
    }

    public ConnectionHook getCloseConnectionHook() {
        return closeConnectionHook;
    }

    public void setCloseConnectionHook(ConnectionHook closeConnectionHook) {
        this.closeConnectionHook = closeConnectionHook;
    }

    public InvocationHook getInvocationHook() {
        return invocationHook;
    }

    public void setInvocationHook(InvocationHook invocationHook) {
        this.invocationHook = invocationHook;
    }

    public ViburLogger getViburLogger() {
        return viburLogger;
    }

    public void setViburLogger(ViburLogger viburLogger) {
        this.viburLogger = viburLogger;
    }

    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    public String takenConnectionsToString() {
        if (!isPoolEnableConnectionTracking())
            return "poolEnableConnectionTracking is disabled.";

        TakenListener<ConnHolder> listener = (TakenListener<ConnHolder>) getPool().listener();
        ConnHolder[] connHolders = listener.getTaken(new ConnHolder[getPoolMaxSize()]);

        int size = 0;
        while (size < connHolders.length && connHolders[size] != null)
            size++;
        if (size == 0)
            return "";
        Arrays.sort(connHolders, 0, size, new Comparator<ConnHolder>() { // sort newest on top
            @Override
            public int compare(ConnHolder h1, ConnHolder h2) {
                long diff = h1.getTakenTime() - h2.getTakenTime();
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });

        long now = System.currentTimeMillis();
        StringBuilder builder = new StringBuilder(8192);
        for (int i = 0; i < size; i++) {
            ConnHolder connHolder = connHolders[i];
            long takenTime = connHolder.getTakenTime();
            Thread thread = connHolder.getThread();
            builder.append(connHolder.value())
                    .append(", taken at ").append(new Date(takenTime)).append(", as millis = ").append(takenTime)
                    .append(", held for ").append(now - takenTime)
                    .append("ms, by thread ").append(thread.getName()).append(", state ").append(thread.getState())
                    .append('\n').append(getStackTraceAsString(connHolder.getLocation().getStackTrace())).append('\n');
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return new StringBuilder(512).append(super.toString())
            .append("[driverClassName = ").append(driverClassName)
            .append(", jdbcUrl = ").append(jdbcUrl)
            .append(", username = ").append(username)
            .append(", externalDataSource = ").append(externalDataSource)
            .append(", poolInitialSize = ").append(poolInitialSize)
            .append(", poolMaxSize = ").append(poolMaxSize)
            .append(", poolFair = ").append(poolFair)
            .append(", pool = ").append(pool)
            .append(", name = ").append(name)
            .append(", statementCacheMaxSize = ").append(statementCacheMaxSize)
            .append(']').toString();
    }
}
