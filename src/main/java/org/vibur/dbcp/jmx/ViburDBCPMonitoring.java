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

import org.vibur.dbcp.ConnState;
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.objectpool.Holder;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPMonitoring implements ViburDBCPMonitoringMBean {

    private final ViburDBCPConfig viburDBCPConfig;

    public ViburDBCPMonitoring(ViburDBCPConfig viburDBCPConfig) {
        this.viburDBCPConfig = viburDBCPConfig;
        initJMX();
    }

    protected void initJMX() {
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
        return viburDBCPConfig.getConnectionPool().taken();
    }

    public int getPoolRemainingCreated() {
        return viburDBCPConfig.getConnectionPool().remainingCreated();
    }

    public boolean isPoolFair() {
        return viburDBCPConfig.isPoolFair();
    }

    public boolean isPoolEnableConnectionTracking() {
        return viburDBCPConfig.isPoolEnableConnectionTracking();
    }

    public long getReducerTimeoutInSeconds() {
        return viburDBCPConfig.getReducerTimeoutInSeconds();
    }

    public float getReducerTakenRatio() {
        return viburDBCPConfig.getReducerTakenRatio();
    }

    public float getReducerReduceRatio() {
        return viburDBCPConfig.getReducerReduceRatio();
    }

    public long getCreateConnectionTimeoutInMs() {
        return viburDBCPConfig.getCreateConnectionTimeoutInMs();
    }

    public void setCreateConnectionTimeoutInMs(long createConnectionTimeoutInMs) {
        viburDBCPConfig.setCreateConnectionTimeoutInMs(createConnectionTimeoutInMs);
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

    public boolean isLogStatementsEnabled() {
        return viburDBCPConfig.isLogStatementsEnabled();
    }

    public void setLogStatementsEnabled(boolean logStatementsEnabled) {
        viburDBCPConfig.setLogStatementsEnabled(logStatementsEnabled);
    }

    public long getQueryExecuteTimeLimitInMs() {
        return viburDBCPConfig.getQueryExecuteTimeLimitInMs();
    }

    public void setQueryExecuteTimeLimitInMs(long queryExecuteTimeLimitInMs) {
        viburDBCPConfig.setQueryExecuteTimeLimitInMs(queryExecuteTimeLimitInMs);
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

        List<Holder<ConnState>> holders = viburDBCPConfig.getConnectionPool().takenHolders();
        Collections.sort(holders, new Comparator<Holder<ConnState>>() { // sort oldest on top
            public int compare(Holder<ConnState> h1, Holder<ConnState> h2) {
                long diff = h1.getTime() - h2.getTime();
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });

        StringBuilder builder = new StringBuilder(4096);
        String newLine = System.getProperty("line.separator");
        for (Holder<ConnState> holder : holders) {
            builder.append("time taken: ").append(new Date(holder.getTime()))
                .append(", millis = ").append(holder.getTime()).append(newLine);
            for (StackTraceElement element : holder.getStackTrace())
                builder.append("  at ").append(element).append(newLine);
            builder.append(newLine);
        }
        return builder.toString();
    }
}
