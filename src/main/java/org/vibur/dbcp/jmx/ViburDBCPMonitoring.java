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
import org.vibur.dbcp.pool.ConnState;
import org.vibur.objectpool.Holder;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static org.vibur.dbcp.util.ViburUtils.NEW_LINE;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPMonitoring implements ViburDBCPMonitoringMBean {

    private final ViburDBCPConfig viburDBCPConfig;

    public ViburDBCPMonitoring(ViburDBCPConfig viburDBCPConfig) throws ViburDBCPException {
        this.viburDBCPConfig = viburDBCPConfig;
        initJMX();
    }

    private void initJMX() throws ViburDBCPException {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("org.vibur.dbcp:type=ViburDBCP-"
                + Integer.toHexString(viburDBCPConfig.hashCode()));
            if (!mbs.isRegistered(name))
                mbs.registerMBean(this, name);
        } catch (JMException e) {
            throw new ViburDBCPException(e);
        }
    }

    public String getJdbcUrl() {
        return viburDBCPConfig.getJdbcUrl();
    }

    public String getDriverClassName() {
        return viburDBCPConfig.getDriverClassName();
    }

    public int getConnectionIdleLimitInSeconds() {
        return viburDBCPConfig.getConnectionIdleLimitInSeconds();
    }

    public void setConnectionIdleLimitInSeconds(int validateIfIdleForSeconds) {
        viburDBCPConfig.setConnectionIdleLimitInSeconds(validateIfIdleForSeconds);
    }

    public String getTestConnectionQuery() {
        return viburDBCPConfig.getTestConnectionQuery();
    }

    public void setTestConnectionQuery(String testConnectionQuery) {
        viburDBCPConfig.setTestConnectionQuery(testConnectionQuery);
    }

    public int getPoolInitialSize() {
        return viburDBCPConfig.getPoolInitialSize();
    }

    public int getPoolMaxSize() {
        return viburDBCPConfig.getPoolMaxSize();
    }

    public int getPoolTaken() {
        return viburDBCPConfig.getPoolOperations().getPool().taken();
    }

    public int getPoolRemainingCreated() {
        return viburDBCPConfig.getPoolOperations().getPool().remainingCreated();
    }

    public boolean isPoolFair() {
        return viburDBCPConfig.isPoolFair();
    }

    public boolean isPoolEnableConnectionTracking() {
        return viburDBCPConfig.isPoolEnableConnectionTracking();
    }

    public long getReducerTimeIntervalInSeconds() {
        return viburDBCPConfig.getReducerTimeIntervalInSeconds();
    }

    public float getReducerSamples() {
        return viburDBCPConfig.getReducerSamples();
    }

    public long getConnectionTimeoutInMs() {
        return viburDBCPConfig.getConnectionTimeoutInMs();
    }

    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        viburDBCPConfig.setConnectionTimeoutInMs(connectionTimeoutInMs);
    }

    public long getAcquireRetryDelayInMs() {
        return viburDBCPConfig.getAcquireRetryDelayInMs();
    }

    public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
        viburDBCPConfig.setAcquireRetryDelayInMs(acquireRetryDelayInMs);
    }

    public int getAcquireRetryAttempts() {
        return viburDBCPConfig.getAcquireRetryAttempts();
    }

    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        viburDBCPConfig.setAcquireRetryAttempts(acquireRetryAttempts);
    }

    public int getStatementCacheMaxSize() {
        return viburDBCPConfig.getStatementCacheMaxSize();
    }

    public long getLogConnectionLongerThanMs() {
        return viburDBCPConfig.getLogConnectionLongerThanMs();
    }

    public void setLogConnectionLongerThanMs(long logConnectionLongerThanMs) {
        viburDBCPConfig.setLogConnectionLongerThanMs(logConnectionLongerThanMs);
    }

    public boolean isLogStackTraceForLongConnection() {
        return viburDBCPConfig.isLogStackTraceForLongConnection();
    }

    public void setLogStackTraceForLongConnection(boolean logStackTraceForLongConnection) {
        viburDBCPConfig.setLogStackTraceForLongConnection(logStackTraceForLongConnection);
    }

    public long getLogQueryExecutionLongerThanMs() {
        return viburDBCPConfig.getLogQueryExecutionLongerThanMs();
    }

    public void setLogQueryExecutionLongerThanMs(long logQueryExecutionLongerThanMs) {
        viburDBCPConfig.setLogQueryExecutionLongerThanMs(logQueryExecutionLongerThanMs);
    }

    public boolean isLogStackTraceForLongQueryExecution() {
        return viburDBCPConfig.isLogStackTraceForLongQueryExecution();
    }

    public void setLogStackTraceForLongQueryExecution(boolean logStackTraceForLongQueryExecution) {
        viburDBCPConfig.setLogStackTraceForLongQueryExecution(logStackTraceForLongQueryExecution);
    }

    public boolean isResetDefaultsAfterUse() {
        return viburDBCPConfig.isResetDefaultsAfterUse();
    }

    public Boolean getDefaultAutoCommit() {
        return viburDBCPConfig.getDefaultAutoCommit();
    }

    public Boolean getDefaultReadOnly() {
        return viburDBCPConfig.getDefaultReadOnly();
    }

    public String getDefaultTransactionIsolation() {
        return viburDBCPConfig.getDefaultTransactionIsolation();
    }

    public String getDefaultCatalog() {
        return viburDBCPConfig.getDefaultCatalog();
    }

    public String showTakenConnections() {
        if (!viburDBCPConfig.isPoolEnableConnectionTracking())
            return null;

        List<Holder<ConnState>> holders = viburDBCPConfig.getPoolOperations().getPool().takenHolders();
        Collections.sort(holders, new Comparator<Holder<ConnState>>() { // sort newest on top
            public int compare(Holder<ConnState> h1, Holder<ConnState> h2) {
                long diff = h2.getTime() - h1.getTime();
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });

        StringBuilder builder = new StringBuilder(4096);
        for (Holder<ConnState> holder : holders) {
            builder.append(holder.value().connection())
                .append(", taken at: ").append(new Date(holder.getTime()))
                .append(", millis = ").append(holder.getTime()).append(NEW_LINE)
                .append(getStackTraceAsString(holder.getStackTrace())).append(NEW_LINE);
        }
        return builder.toString();
    }
}
