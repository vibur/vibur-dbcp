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

import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.objectpool.listener.TakenListener;

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
public class ViburDBCPMonitoring implements ViburDBCPMonitoringMBean {

    private final ViburDBCPConfig config;

    public ViburDBCPMonitoring(ViburDBCPConfig config) throws ViburDBCPException {
        this.config = config;
        initJMX();
    }

    private void initJMX() throws ViburDBCPException {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("org.vibur.dbcp:type=ViburDBCP-" + config.getName());
            if (!mbs.isRegistered(name))
                mbs.registerMBean(this, name);
        } catch (JMException e) {
            throw new ViburDBCPException(e);
        }
    }

    public String getJdbcUrl() {
        return config.getJdbcUrl();
    }

    public String getDriverClassName() {
        return config.getDriverClassName();
    }

    public int getConnectionIdleLimitInSeconds() {
        return config.getConnectionIdleLimitInSeconds();
    }

    public void setConnectionIdleLimitInSeconds(int connectionIdleLimitInSeconds) {
        config.setConnectionIdleLimitInSeconds(connectionIdleLimitInSeconds);
    }

    public int getValidateTimeoutInSeconds() {
        return config.getValidateTimeoutInSeconds();
    }

    public void setValidateTimeoutInSeconds(int validateTimeoutInSeconds) {
        config.setValidateTimeoutInSeconds(validateTimeoutInSeconds);
    }

    public String getTestConnectionQuery() {
        return config.getTestConnectionQuery();
    }

    public void setTestConnectionQuery(String testConnectionQuery) {
        config.setTestConnectionQuery(testConnectionQuery);
    }

    public String getInitSQL() {
        return config.getInitSQL();
    }

    public void setInitSQL(String initSQL) {
        config.setInitSQL(initSQL);
    }

    public int getPoolInitialSize() {
        return config.getPoolInitialSize();
    }

    public int getPoolMaxSize() {
        return config.getPoolMaxSize();
    }

    public int getPoolTaken() {
        return config.getPool().taken();
    }

    public int getPoolRemainingCreated() {
        return config.getPool().remainingCreated();
    }

    public boolean isPoolFair() {
        return config.isPoolFair();
    }

    public boolean isPoolEnableConnectionTracking() {
        return config.isPoolEnableConnectionTracking();
    }

    public int getReducerTimeIntervalInSeconds() {
        return config.getReducerTimeIntervalInSeconds();
    }

    public int getReducerSamples() {
        return config.getReducerSamples();
    }

    public long getConnectionTimeoutInMs() {
        return config.getConnectionTimeoutInMs();
    }

    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        config.setConnectionTimeoutInMs(connectionTimeoutInMs);
    }

    public int getLoginTimeoutInSeconds() {
        return config.getLoginTimeoutInSeconds();
    }

    public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
        config.setLoginTimeoutInSeconds(loginTimeoutInSeconds);
    }

    public long getAcquireRetryDelayInMs() {
        return config.getAcquireRetryDelayInMs();
    }

    public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
        config.setAcquireRetryDelayInMs(acquireRetryDelayInMs);
    }

    public int getAcquireRetryAttempts() {
        return config.getAcquireRetryAttempts();
    }

    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        config.setAcquireRetryAttempts(acquireRetryAttempts);
    }

    public int getStatementCacheMaxSize() {
        return config.getStatementCacheMaxSize();
    }

    public long getLogConnectionLongerThanMs() {
        return config.getLogConnectionLongerThanMs();
    }

    public void setLogConnectionLongerThanMs(long logConnectionLongerThanMs) {
        config.setLogConnectionLongerThanMs(logConnectionLongerThanMs);
    }

    public boolean isLogStackTraceForLongConnection() {
        return config.isLogStackTraceForLongConnection();
    }

    public void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection) {
        config.setLogStackTraceForLongConnection(logStackTraceForLongConnection);
    }

    public long getLogQueryExecutionLongerThanMs() {
        return config.getLogQueryExecutionLongerThanMs();
    }

    public void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs) {
        config.setLogQueryExecutionLongerThanMs(logQueryExecutionLongerThanMs);
    }

    public boolean isLogStackTraceForLongQueryExecution() {
        return config.isLogStackTraceForLongQueryExecution();
    }

    public void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution) {
        config.setLogStackTraceForLongQueryExecution(logStackTraceForLongQueryExecution);
    }

    public long getLogLargeResultSet() {
        return config.getLogLargeResultSet();
    }

    public void setLogLargeResultSet(long logLargeResultSet) {
        config.setLogLargeResultSet(logLargeResultSet);
    }

    public boolean isLogStackTraceForLargeResultSet() {
        return config.isLogStackTraceForLargeResultSet();
    }

    public void setLogStackTraceForLargeResultSet(boolean logStackTraceForLargeResultSet) {
        config.setLogStackTraceForLargeResultSet(logStackTraceForLargeResultSet);
    }

    public boolean isResetDefaultsAfterUse() {
        return config.isResetDefaultsAfterUse();
    }

    public Boolean getDefaultAutoCommit() {
        return config.getDefaultAutoCommit();
    }

    public Boolean getDefaultReadOnly() {
        return config.getDefaultReadOnly();
    }

    public String getDefaultTransactionIsolation() {
        return config.getDefaultTransactionIsolation();
    }

    public String getDefaultCatalog() {
        return config.getDefaultCatalog();
    }

    public boolean isClearSQLWarnings() {
        return config.isClearSQLWarnings();
    }

    public String showTakenConnections() {
        if (!config.isPoolEnableConnectionTracking())
            return "poolEnableConnectionTracking is disabled.";

        List<ConnHolder> connHolders = ((TakenListener<ConnHolder>) config.getPool().listener()).getTaken();
        Collections.sort(connHolders, new Comparator<ConnHolder>() { // sort newest on top
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
