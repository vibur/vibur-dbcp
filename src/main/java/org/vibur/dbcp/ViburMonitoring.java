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

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author Simeon Malchev
 */
public final class ViburMonitoring implements ViburMonitoringMBean {

    private static final Logger logger = LoggerFactory.getLogger(ViburMonitoring.class);

    private final ViburConfig config;

    private ViburMonitoring(ViburConfig config) {
        this.config = config;
    }

    static void registerMBean(ViburConfig config) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(config.getJmxName());
            if (!mbs.isRegistered(objectName))
                mbs.registerMBean(new ViburMonitoring(config), objectName);
            else
                logger.warn(config.getJmxName() + " is already registered.");
        } catch (JMException e) {
            logger.warn("Unable to register mBean {}", config.getJmxName(), e);
        }
    }

    static void unregisterMBean(ViburConfig config) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(config.getJmxName());
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
            else
                logger.warn(config.getJmxName() + " is not registered.");
        } catch (JMException e) {
            logger.warn("Unable to unregister mBean {}", config.getJmxName(), e);
        }
    }

    @Override
    public String getJdbcUrl() {
        return config.getJdbcUrl();
    }

    @Override
    public String getDriverClassName() {
        return config.getDriverClassName();
    }

    @Override
    public int getConnectionIdleLimitInSeconds() {
        return config.getConnectionIdleLimitInSeconds();
    }

    @Override
    public void setConnectionIdleLimitInSeconds(int connectionIdleLimitInSeconds) {
        config.setConnectionIdleLimitInSeconds(connectionIdleLimitInSeconds);
    }

    @Override
    public int getValidateTimeoutInSeconds() {
        return config.getValidateTimeoutInSeconds();
    }

    @Override
    public void setValidateTimeoutInSeconds(int validateTimeoutInSeconds) {
        config.setValidateTimeoutInSeconds(validateTimeoutInSeconds);
    }

    @Override
    public String getTestConnectionQuery() {
        return config.getTestConnectionQuery();
    }

    @Override
    public void setTestConnectionQuery(String testConnectionQuery) {
        config.setTestConnectionQuery(testConnectionQuery);
    }

    @Override
    public String getInitSQL() {
        return config.getInitSQL();
    }

    @Override
    public void setInitSQL(String initSQL) {
        config.setInitSQL(initSQL);
    }

    @Override
    public boolean isUseNetworkTimeout() {
        return config.isUseNetworkTimeout();
    }

    @Override
    public int getPoolInitialSize() {
        return config.getPoolInitialSize();
    }

    @Override
    public int getPoolMaxSize() {
        return config.getPoolMaxSize();
    }

    @Override
    public int getPoolTaken() {
        return config.getPool().taken();
    }

    @Override
    public int getPoolRemainingCreated() {
        return config.getPool().remainingCreated();
    }

    @Override
    public boolean isPoolFair() {
        return config.isPoolFair();
    }

    @Override
    public boolean isPoolEnableConnectionTracking() {
        return config.isPoolEnableConnectionTracking();
    }

    @Override
    public int getReducerTimeIntervalInSeconds() {
        return config.getReducerTimeIntervalInSeconds();
    }

    @Override
    public int getReducerSamples() {
        return config.getReducerSamples();
    }

    @Override
    public boolean isAllowConnectionAfterTermination() {
        return config.isAllowConnectionAfterTermination();
    }

    @Override
    public boolean isAllowUnwrapping() {
        return config.isAllowUnwrapping();
    }

    @Override
    public long getConnectionTimeoutInMs() {
        return config.getConnectionTimeoutInMs();
    }

    @Override
    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        config.setConnectionTimeoutInMs(connectionTimeoutInMs);
    }

    @Override
    public int getLoginTimeoutInSeconds() {
        return config.getLoginTimeoutInSeconds();
    }

    @Override
    public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
        config.setLoginTimeoutInSeconds(loginTimeoutInSeconds);
    }

    @Override
    public long getAcquireRetryDelayInMs() {
        return config.getAcquireRetryDelayInMs();
    }

    @Override
    public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
        config.setAcquireRetryDelayInMs(acquireRetryDelayInMs);
    }

    @Override
    public int getAcquireRetryAttempts() {
        return config.getAcquireRetryAttempts();
    }

    @Override
    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        config.setAcquireRetryAttempts(acquireRetryAttempts);
    }

    @Override
    public int getStatementCacheMaxSize() {
        return config.getStatementCacheMaxSize();
    }

    @Override
    public long getLogConnectionLongerThanMs() {
        return config.getLogConnectionLongerThanMs();
    }

    @Override
    public void setLogConnectionLongerThanMs(long logConnectionLongerThanMs) {
        config.setLogConnectionLongerThanMs(logConnectionLongerThanMs);
    }

    @Override
    public boolean isLogStackTraceForLongConnection() {
        return config.isLogStackTraceForLongConnection();
    }

    @Override
    public void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection) {
        config.setLogStackTraceForLongConnection(logStackTraceForLongConnection);
    }

    @Override
    public long getLogQueryExecutionLongerThanMs() {
        return config.getLogQueryExecutionLongerThanMs();
    }

    @Override
    public void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs) {
        config.setLogQueryExecutionLongerThanMs(logQueryExecutionLongerThanMs);
    }

    @Override
    public boolean isLogStackTraceForLongQueryExecution() {
        return config.isLogStackTraceForLongQueryExecution();
    }

    @Override
    public void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution) {
        config.setLogStackTraceForLongQueryExecution(logStackTraceForLongQueryExecution);
    }

    @Override
    public long getLogLargeResultSet() {
        return config.getLogLargeResultSet();
    }

    @Override
    public void setLogLargeResultSet(long logLargeResultSet) {
        config.setLogLargeResultSet(logLargeResultSet);
    }

    @Override
    public boolean isLogStackTraceForLargeResultSet() {
        return config.isLogStackTraceForLargeResultSet();
    }

    @Override
    public void setLogStackTraceForLargeResultSet(boolean logStackTraceForLargeResultSet) {
        config.setLogStackTraceForLargeResultSet(logStackTraceForLargeResultSet);
    }

    @Override
    public boolean isIncludeQueryParameters() {
        return config.isIncludeQueryParameters();
    }

    @Override
    public void setIncludeQueryParameters(boolean includeQueryParameters) {
        config.setIncludeQueryParameters(includeQueryParameters);
    }

    @Override
    public boolean isLogTakenConnectionsOnTimeout() {
        return config.isLogTakenConnectionsOnTimeout();
    }

    @Override
    public boolean isResetDefaultsAfterUse() {
        return config.isResetDefaultsAfterUse();
    }

    @Override
    public Boolean getDefaultAutoCommit() {
        return config.getDefaultAutoCommit();
    }

    @Override
    public Boolean getDefaultReadOnly() {
        return config.getDefaultReadOnly();
    }

    @Override
    public String getDefaultTransactionIsolation() {
        return config.getDefaultTransactionIsolation();
    }

    @Override
    public String getDefaultCatalog() {
        return config.getDefaultCatalog();
    }

    @Override
    public boolean isClearSQLWarnings() {
        return config.isClearSQLWarnings();
    }

    @Override
    public String showTakenConnections() {
        return config.takenConnectionsToString();
    }
}
