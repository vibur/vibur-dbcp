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

/**
 * @author Simeon Malchev
 */
public interface ViburDBCPConfigMBean {

    String getJdbcUrl();


    int getConnectionIdleLimitInSeconds();

    void setConnectionIdleLimitInSeconds(int validateIfIdleForSeconds);

    String getTestConnectionQuery();

    void setTestConnectionQuery(String testConnectionQuery);


    int getPoolInitialSize();

    int getPoolMaxSize();

    int getPoolTaken();

    int getPoolRemainingCreated();

    boolean isPoolFair();

    boolean isPoolEnableConnectionTracking();

    long getReducerTimeoutInSeconds();

    float getReducerTakenRatio();

    float getReducerReduceRatio();


    long getCreateConnectionTimeoutInMs();

    void setCreateConnectionTimeoutInMs(long createConnectionTimeoutInMs);

    long getAcquireRetryDelayInMs();

    void setAcquireRetryDelayInMs(long acquireRetryDelayInMs);

    int getAcquireRetryAttempts();

    void setAcquireRetryAttempts(int acquireRetryAttempts);


    int getStatementCacheMaxSize();


    boolean isLogStatementsEnabled();

    void setLogStatementsEnabled(boolean logStatementsEnabled);

    long getQueryExecuteTimeLimitInMs();

    void setQueryExecuteTimeLimitInMs(long queryExecuteTimeLimitInMs);


    boolean isResetDefaultsAfterUse();

    Boolean getDefaultAutoCommit();

    Boolean getDefaultReadOnly();

    String getDefaultTransactionIsolation();

    String getDefaultCatalog();
}
