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

    private final ViburDBCPDataSource dataSource;

    private ViburMonitoring(ViburDBCPDataSource dataSource) {
        this.dataSource = dataSource;
    }

    static void registerMBean(ViburDBCPDataSource dataSource) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(dataSource.getJmxName());
            if (!mbs.isRegistered(objectName))
                mbs.registerMBean(new ViburMonitoring(dataSource), objectName);
            else
                logger.warn(dataSource.getJmxName() + " is already registered.");
        } catch (JMException e) {
            logger.warn("Unable to register mBean {}", dataSource.getJmxName(), e);
        }
    }

    static void unregisterMBean(ViburDBCPDataSource dataSource) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(dataSource.getJmxName());
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
            else
                logger.debug(dataSource.getJmxName() + " is not registered.");
        } catch (JMException e) {
            logger.warn("Unable to unregister mBean {}", dataSource.getJmxName(), e);
        }
    }

    @Override
    public String getJdbcUrl() {
        return dataSource.getJdbcUrl();
    }

    @Override
    public String getDriverClassName() {
        return dataSource.getDriverClassName();
    }

    @Override
    public int getConnectionIdleLimitInSeconds() {
        return dataSource.getConnectionIdleLimitInSeconds();
    }

    @Override
    public void setConnectionIdleLimitInSeconds(int connectionIdleLimitInSeconds) {
        dataSource.setConnectionIdleLimitInSeconds(connectionIdleLimitInSeconds);
    }

    @Override
    public int getValidateTimeoutInSeconds() {
        return dataSource.getValidateTimeoutInSeconds();
    }

    @Override
    public void setValidateTimeoutInSeconds(int validateTimeoutInSeconds) {
        dataSource.setValidateTimeoutInSeconds(validateTimeoutInSeconds);
    }

    @Override
    public String getTestConnectionQuery() {
        return dataSource.getTestConnectionQuery();
    }

    @Override
    public void setTestConnectionQuery(String testConnectionQuery) {
        dataSource.setTestConnectionQuery(testConnectionQuery);
    }

    @Override
    public String getInitSQL() {
        return dataSource.getInitSQL();
    }

    @Override
    public void setInitSQL(String initSQL) {
        dataSource.setInitSQL(initSQL);
    }

    @Override
    public boolean isUseNetworkTimeout() {
        return dataSource.isUseNetworkTimeout();
    }

    @Override
    public int getPoolInitialSize() {
        return dataSource.getPoolInitialSize();
    }

    @Override
    public int getPoolMaxSize() {
        return dataSource.getPoolMaxSize();
    }

    @Override
    public int getPoolTaken() {
        return dataSource.getPool().taken();
    }

    @Override
    public int getPoolRemainingCreated() {
        return dataSource.getPool().remainingCreated();
    }

    @Override
    public boolean isPoolFair() {
        return dataSource.isPoolFair();
    }

    @Override
    public boolean isPoolEnableConnectionTracking() {
        return dataSource.isPoolEnableConnectionTracking();
    }

    @Override
    public int getReducerTimeIntervalInSeconds() {
        return dataSource.getReducerTimeIntervalInSeconds();
    }

    @Override
    public int getReducerSamples() {
        return dataSource.getReducerSamples();
    }

    @Override
    public boolean isAllowConnectionAfterTermination() {
        return dataSource.isAllowConnectionAfterTermination();
    }

    @Override
    public boolean isAllowUnwrapping() {
        return dataSource.isAllowUnwrapping();
    }

    @Override
    public long getConnectionTimeoutInMs() {
        return dataSource.getConnectionTimeoutInMs();
    }

    @Override
    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        dataSource.setConnectionTimeoutInMs(connectionTimeoutInMs);
    }

    @Override
    public int getLoginTimeoutInSeconds() {
        return dataSource.getLoginTimeoutInSeconds();
    }

    @Override
    public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
        dataSource.setLoginTimeoutInSeconds(loginTimeoutInSeconds);
    }

    @Override
    public long getAcquireRetryDelayInMs() {
        return dataSource.getAcquireRetryDelayInMs();
    }

    @Override
    public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
        dataSource.setAcquireRetryDelayInMs(acquireRetryDelayInMs);
    }

    @Override
    public int getAcquireRetryAttempts() {
        return dataSource.getAcquireRetryAttempts();
    }

    @Override
    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        dataSource.setAcquireRetryAttempts(acquireRetryAttempts);
    }

    @Override
    public int getStatementCacheMaxSize() {
        return dataSource.getStatementCacheMaxSize();
    }

    @Override
    public long getLogConnectionLongerThanMs() {
        return dataSource.getLogConnectionLongerThanMs();
    }

    @Override
    public void setLogConnectionLongerThanMs(long logConnectionLongerThanMs) {
        dataSource.setLogConnectionLongerThanMs(logConnectionLongerThanMs);
    }

    @Override
    public boolean isLogStackTraceForLongConnection() {
        return dataSource.isLogStackTraceForLongConnection();
    }

    @Override
    public void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection) {
        dataSource.setLogStackTraceForLongConnection(logStackTraceForLongConnection);
    }

    @Override
    public long getLogQueryExecutionLongerThanMs() {
        return dataSource.getLogQueryExecutionLongerThanMs();
    }

    @Override
    public void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs) {
        dataSource.setLogQueryExecutionLongerThanMs(logQueryExecutionLongerThanMs);
    }

    @Override
    public boolean isLogStackTraceForLongQueryExecution() {
        return dataSource.isLogStackTraceForLongQueryExecution();
    }

    @Override
    public void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution) {
        dataSource.setLogStackTraceForLongQueryExecution(logStackTraceForLongQueryExecution);
    }

    @Override
    public long getLogLargeResultSet() {
        return dataSource.getLogLargeResultSet();
    }

    @Override
    public void setLogLargeResultSet(long logLargeResultSet) {
        dataSource.setLogLargeResultSet(logLargeResultSet);
    }

    @Override
    public boolean isLogStackTraceForLargeResultSet() {
        return dataSource.isLogStackTraceForLargeResultSet();
    }

    @Override
    public void setLogStackTraceForLargeResultSet(boolean logStackTraceForLargeResultSet) {
        dataSource.setLogStackTraceForLargeResultSet(logStackTraceForLargeResultSet);
    }

    @Override
    public boolean isIncludeQueryParameters() {
        return dataSource.isIncludeQueryParameters();
    }

    @Override
    public void setIncludeQueryParameters(boolean includeQueryParameters) {
        dataSource.setIncludeQueryParameters(includeQueryParameters);
    }

    @Override
    public boolean isLogTakenConnectionsOnTimeout() {
        return dataSource.isLogTakenConnectionsOnTimeout();
    }

    @Override
    public boolean isLogAllStackTracesOnTimeout() {
        return dataSource.isLogAllStackTracesOnTimeout();
    }

    @Override
    public boolean isResetDefaultsAfterUse() {
        return dataSource.isResetDefaultsAfterUse();
    }

    @Override
    public Boolean getDefaultAutoCommit() {
        return dataSource.getDefaultAutoCommit();
    }

    @Override
    public Boolean getDefaultReadOnly() {
        return dataSource.getDefaultReadOnly();
    }

    @Override
    public String getDefaultTransactionIsolation() {
        return dataSource.getDefaultTransactionIsolation();
    }

    @Override
    public String getDefaultCatalog() {
        return dataSource.getDefaultCatalog();
    }

    @Override
    public boolean isClearSQLWarnings() {
        return dataSource.isClearSQLWarnings();
    }

    @Override
    public String showTakenConnections() {
        return dataSource.getTakenConnectionsStackTraces();
    }
}
