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

import vibur.dbcp.cache.ConcurrentCache;
import vibur.dbcp.proxy.StatementDescriptor;

import java.sql.Statement;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPConfig {

    /** Database driver class name */
    private String driverClassName;
    /** Database JDBC Connection string. */
    private String jdbcUrl;
    /** User name to use. */
    private String username;
    /** Password to use. */
    private String password;


    /** If set to true, will validate the taken from the pool JDBC Connections before to give them to
     * the application. */
    private boolean validateOnTake = false;
    /** If set to true, will validate the returned by the application JDBC Connection before to restore
     * it in the pool. */
    private boolean validateOnRestore = false;
    /** Used to test the validity of the JDBC Connection. Should be set to a valid query if any of the
     * {@code validateOnTake} or {@code validateOnRestore} are set to {@code true}. */
    private String testConnectionQuery = "SELECT 1";


    /** The pool initial size, i.e. the initial number of JDBC Connections allocated in this pool. */
    private int poolInitialSize = 1;
    /** The pool max size, i.e. the maximum number of JDBC Connections allocated in this pool. */
    private int poolMaxSize = 10;
    /** The pool's fairness setting with regards to waiting threads. */
    private boolean poolFair = false;
    /** If true, the pool will keep information for the current stack trace of every taken connection. */
    private boolean poolEnableConnectionTracking = false;


    /** For more details on the next 3 parameters see {@link vibur.object_pool.util.PoolReducer}
     *  and {@link vibur.object_pool.util.DefaultReducer}
     */
    /**
     * The time periods after which the {@code PoolReducer} will wake up. */
    private long reducerTimeoutInSeconds = 30;
    /** The ratio between the taken objects from the pool and the available objects in the pool. */
    private float reducerTakenRatio = 0.90f;
    /** The ratio by which the number of available in the pool objects is to be reduced if the above
     *  {@code reducerTakenRatio} threshold is hit. */
    private float reducerReduceRatio = 0.10f;


    /** Time to wait before a call to getConnection() times out and returns an error.
     * {@code 0} means forever */
    private long createConnectionTimeoutInMs = 0;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * wait for this value before attempting to acquire a new JDBC Connection again. */
    private long acquireRetryDelayInMs = 1000;
    /** After attempting to acquire a JDBC Connection and failing with an {@code SQLException},
     * try to connect these many times before giving up. */
    private int acquireRetryAttempts = 3;


    /** Defines the maximum statement cache size. {@code 0} disables it, max values is {@code 1000} */
    private int statementCacheMaxSize = 0;
    private ConcurrentCache<StatementDescriptor, Statement> statementCache = null;

    /** If set to true, log all SQL statements being executed. */
    private boolean logStatementsEnabled = false;
    /** Queries taking longer than this limit to execute are logged. {@code 0} disables it. */
    private long queryExecuteTimeLimitInMs = 0;


    /** The default auto-commit state of created connections. */
    private Boolean defaultAutoCommit;
    /** The default read-only state of created connections. */
    private Boolean defaultReadOnly;
    /** The default transaction isolation state of created connections. */
    private String defaultTransactionIsolation;
    /** The default catalog state of created connections. */
    private String defaultCatalog;
    /** The parsed transaction isolation value. Default = driver value*/
    private Integer defaultTransactionIsolationValue;


    //////////////////////// Getter & Setters ////////////////////////

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

    public boolean isValidateOnTake() {
        return validateOnTake;
    }

    public void setValidateOnTake(boolean validateOnTake) {
        this.validateOnTake = validateOnTake;
    }

    public boolean isValidateOnRestore() {
        return validateOnRestore;
    }

    public void setValidateOnRestore(boolean validateOnRestore) {
        this.validateOnRestore = validateOnRestore;
    }

    public String getTestConnectionQuery() {
        return testConnectionQuery;
    }

    public void setTestConnectionQuery(String testConnectionQuery) {
        this.testConnectionQuery = testConnectionQuery;
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

    public long getReducerTimeoutInSeconds() {
        return reducerTimeoutInSeconds;
    }

    public void setReducerTimeoutInSeconds(long reducerTimeoutInSeconds) {
        this.reducerTimeoutInSeconds = reducerTimeoutInSeconds;
    }

    public float getReducerTakenRatio() {
        return reducerTakenRatio;
    }

    public void setReducerTakenRatio(float reducerTakenRatio) {
        this.reducerTakenRatio = reducerTakenRatio;
    }

    public float getReducerReduceRatio() {
        return reducerReduceRatio;
    }

    public void setReducerReduceRatio(float reducerReduceRatio) {
        this.reducerReduceRatio = reducerReduceRatio;
    }

    public long getCreateConnectionTimeoutInMs() {
        return createConnectionTimeoutInMs;
    }

    public void setCreateConnectionTimeoutInMs(long createConnectionTimeoutInMs) {
        this.createConnectionTimeoutInMs = createConnectionTimeoutInMs;
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

    public ConcurrentCache<StatementDescriptor, Statement> getStatementCache() {
        return statementCache;
    }

    public void setStatementCache(ConcurrentCache<StatementDescriptor, Statement> statementCache) {
        this.statementCache = statementCache;
    }

    public boolean isLogStatementsEnabled() {
        return logStatementsEnabled;
    }

    public void setLogStatementsEnabled(boolean logStatementsEnabled) {
        this.logStatementsEnabled = logStatementsEnabled;
    }

    public long getQueryExecuteTimeLimitInMs() {
        return queryExecuteTimeLimitInMs;
    }

    public void setQueryExecuteTimeLimitInMs(long queryExecuteTimeLimitInMs) {
        this.queryExecuteTimeLimitInMs = queryExecuteTimeLimitInMs;
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
}
