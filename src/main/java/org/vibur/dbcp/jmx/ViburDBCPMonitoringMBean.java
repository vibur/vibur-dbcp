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

package org.vibur.dbcp.jmx;

/**
 * Defines the Vibur DBCP JMX operations.
 *
 * @author Simeon Malchev
 */
public interface ViburDBCPMonitoringMBean {

    //////////// Database connectivity ////////////

    String getJdbcUrl();

    String getDriverClassName();


    //////////// JDBC connection testing ////////////

    int getConnectionIdleLimitInSeconds();

    void setConnectionIdleLimitInSeconds(int validateIfIdleForSeconds);

    String getTestConnectionQuery();

    void setTestConnectionQuery(String testConnectionQuery);


    //////////// Pool parameters and PoolReducer parameters ////////////

    int getPoolInitialSize();

    int getPoolMaxSize();

    int getPoolTaken();

    int getPoolRemainingCreated();

    boolean isPoolFair();

    boolean isPoolEnableConnectionTracking();

    long getReducerTimeIntervalInSeconds();

    float getReducerSamples();


    //////////// JDBC Connection acquiring timeouts and retries ////////////

    long getConnectionTimeoutInMs();

    void setConnectionTimeoutInMs(long connectionTimeoutInMs);

    long getAcquireRetryDelayInMs();

    void setAcquireRetryDelayInMs(long acquireRetryDelayInMs);

    int getAcquireRetryAttempts();

    void setAcquireRetryAttempts(int acquireRetryAttempts);


    //////////// JDBC Statement caching ////////////

    int getStatementCacheMaxSize();


    //////////// JDBC Connection acquiring logging and SQL query execution logging ////////////

    long getLogConnectionLongerThanMs();

    void setLogConnectionLongerThanMs(long logConnectionLongerThanMs);

    boolean isLogStackTraceForLongConnection();

    void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection);

    long getLogQueryExecutionLongerThanMs();

    void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs);

    boolean isLogStackTraceForLongQueryExecution();

    void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution);


    //////////// JDBC Connection default states ////////////

    boolean isResetDefaultsAfterUse();

    Boolean getDefaultAutoCommit();

    Boolean getDefaultReadOnly();

    String getDefaultTransactionIsolation();

    String getDefaultCatalog();


    //////////// Taken JDBC Connections information ////////////

    String showTakenConnections();
}
