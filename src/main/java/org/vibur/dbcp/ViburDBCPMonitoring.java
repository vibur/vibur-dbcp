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
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.TakenListener;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public final class ViburDBCPMonitoring implements ViburMonitoringMBean {

    private static final Logger logger = LoggerFactory.getLogger(ViburDBCPMonitoring.class);

    private final ViburDBCPConfig config;

    private ViburDBCPMonitoring(ViburDBCPConfig config) {
        this.config = config;
    }

    static void registerMBean(ViburDBCPConfig config) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(config.getJmxName());
            if (!mbs.isRegistered(objectName))
                mbs.registerMBean(new ViburDBCPMonitoring(config), objectName);
            else
                logger.warn(config.getJmxName() + " is already registered.");
        } catch (JMException e) {
            logger.warn("Unable to register mBean {}", config.getJmxName(), e);
        }
    }

    static void unregisterMBean(ViburDBCPConfig config) {
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
    @SuppressWarnings("unchecked")
    public String showTakenConnections() {
        if (!config.isPoolEnableConnectionTracking())
            return "poolEnableConnectionTracking is disabled.";

        PoolService<ConnHolder> poolService = (PoolService<ConnHolder>) config.getPool();
        List<ConnHolder> connHolders = ((TakenListener<ConnHolder>) poolService.listener()).getTaken();
        Collections.sort(connHolders, new Comparator<ConnHolder>() { // sort newest on top
            @Override
            public int compare(ConnHolder h1, ConnHolder h2) {
                long diff = h2.getTakenTime() - h1.getTakenTime();
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });

        StringBuilder builder = new StringBuilder(4096);
        for (ConnHolder connHolder : connHolders) {
            builder.append(connHolder.value())
                .append(", taken at: ").append(new Date(connHolder.getTakenTime()))
                .append(", millis = ").append(connHolder.getTakenTime()).append('\n')
                .append(getStackTraceAsString(connHolder.getStackTrace())).append('\n');
        }
        return builder.toString();
    }
}
