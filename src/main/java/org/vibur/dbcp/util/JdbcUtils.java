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

package org.vibur.dbcp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.ViburDBCPException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executor;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.vibur.dbcp.ViburConfig.IS_VALID_QUERY;

/**
 * This class encapsulates all low-level JDBC operations invoked on raw JDBC objects such as
 * rawConnection or rawStatement, plus operations related to JDBC Driver or DataSource initialization
 * and Connection creation.
 *
 * @author Simeon Malchev
 */
public final class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {}

    public static void initLoginTimeout(ViburConfig config) throws ViburDBCPException {
        int loginTimeout = config.getLoginTimeoutInSeconds();
        if (config.getExternalDataSource() == null)
            DriverManager.setLoginTimeout(loginTimeout);
        else {
            try {
                config.getExternalDataSource().setLoginTimeout(loginTimeout);
            } catch (SQLException e) {
                throw new ViburDBCPException(e);
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    public static void setDefaultValues(Connection rawConnection, ViburConfig config) throws SQLException {
        if (config.getDefaultAutoCommit() != null)
            rawConnection.setAutoCommit(config.getDefaultAutoCommit());
        if (config.getDefaultReadOnly() != null)
            rawConnection.setReadOnly(config.getDefaultReadOnly());
        if (config.getDefaultTransactionIsolationValue() != null)
            rawConnection.setTransactionIsolation(config.getDefaultTransactionIsolationValue());
        if (config.getDefaultCatalog() != null)
            rawConnection.setCatalog(config.getDefaultCatalog());
    }

    public static boolean validateConnection(Connection rawConnection, String query, ViburConfig config) throws SQLException {
        if (query == null)
            return true;

        if (query.equals(IS_VALID_QUERY))
            return rawConnection.isValid(config.getValidateTimeoutInSeconds());
        return executeValidationQuery(rawConnection, query, config);
    }

    private static boolean executeValidationQuery(Connection rawConnection, String query, ViburConfig config) throws SQLException {
        int oldTimeout = setNetworkTimeoutIfDifferent(rawConnection, config);

        Statement rawStatement = null;
        try {
            rawStatement = rawConnection.createStatement();
            rawStatement.setQueryTimeout(config.getValidateTimeoutInSeconds());
            rawStatement.execute(query);
        } finally {
            quietClose(rawStatement);
        }

        resetNetworkTimeout(rawConnection, config.getNetworkTimeoutExecutor(), oldTimeout);
        return true;
    }

    private static int setNetworkTimeoutIfDifferent(Connection rawConnection, ViburConfig config) throws SQLException {
        if (config.isUseNetworkTimeout()) {
            int newTimeout = (int) SECONDS.toMillis(config.getValidateTimeoutInSeconds());
            int oldTimeout = rawConnection.getNetworkTimeout();
            if (newTimeout != oldTimeout) {
                rawConnection.setNetworkTimeout(config.getNetworkTimeoutExecutor(), newTimeout);
                return oldTimeout;
            }
        }
        return -1;
    }

    private static void resetNetworkTimeout(Connection rawConnection, Executor executor, int oldTimeout) throws SQLException {
        if (oldTimeout >= 0)
            rawConnection.setNetworkTimeout(executor, oldTimeout);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    public static void clearWarnings(Connection rawConnection) throws SQLException {
        rawConnection.clearWarnings();
    }

    public static void clearWarnings(Statement rawStatement) throws SQLException {
        rawStatement.clearWarnings();
    }

    public static void quietClose(Connection rawConnection) {
        doQuietClose(rawConnection);
    }

    public static void quietClose(Statement rawStatement) {
        doQuietClose(rawStatement);
    }

    private static void doQuietClose(AutoCloseable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close {}", closeable, e);
        } catch (Exception e) {
            logger.warn("Ignoring unexpected exception thrown by the JDBC driver for {}", closeable, e);
        }
    }
}
