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

import java.sql.*;
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

    private JdbcUtils() { }

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
        if (config.getDefaultTransactionIsolationIntValue() != null)
            // noinspection MagicConstant - the int value is checked/ set during Vibur config validation
            rawConnection.setTransactionIsolation(config.getDefaultTransactionIsolationIntValue());
        if (config.getDefaultCatalog() != null)
            rawConnection.setCatalog(config.getDefaultCatalog());
    }

    /**
     * Validates/ initializes the given {@code rawConnection} via executing the given {@code sqlQuery}.
     *
     * @param rawConnection the raw connection to validate/ initialize
     * @param sqlQuery must be a valid SQL query, a special value of {@code isValid} in which case
     *                 the {@link Connection#isValid} method will be called, or {@code null}
     *                 in which case no validation/ initialization will be performed
     * @param config the Vibur config
     * @return {@code true} if the given connection is successfully validated/ initialized; {@code false} otherwise
     */
    public static boolean validateOrInitialize(Connection rawConnection, String sqlQuery, ViburConfig config) {
        if (sqlQuery == null)
            return true;

        try {
            if (sqlQuery.equals(IS_VALID_QUERY))
                return rawConnection.isValid(config.getValidateTimeoutInSeconds());

            executeSqlQuery(rawConnection, sqlQuery, config);
            return true;
        } catch (SQLException e) {
            logger.debug("Couldn't validate/ initialize rawConnection {}", rawConnection, e);
            return false;
        }
    }

    private static void executeSqlQuery(Connection rawConnection, String sqlQuery, ViburConfig config) throws SQLException {
        int oldTimeout = setNetworkTimeoutIfDifferent(rawConnection, config);

        Statement rawStatement = null;
        try {
            rawStatement = rawConnection.createStatement();
            rawStatement.setQueryTimeout(config.getValidateTimeoutInSeconds());
            rawStatement.execute(sqlQuery);
        } finally {
            quietClose(rawStatement);
        }

        resetNetworkTimeout(rawConnection, config.getNetworkTimeoutExecutor(), oldTimeout);
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

    public static void clearWarnings(Connection connection) throws SQLException {
        if (connection != null)
            connection.clearWarnings();
    }

    public static void clearWarnings(PreparedStatement preparedStatement) throws SQLException {
        if (preparedStatement != null)
            preparedStatement.clearWarnings();
    }

    public static void quietClose(Connection connection) {
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            logger.warn("Couldn't close {}", connection, e);
        }
    }

    public static void quietClose(Statement statement) {
        try {
            if (statement != null)
                statement.close();
        } catch (SQLException e) {
            logger.warn("Couldn't close {}", statement, e);
        }
    }

    public static void quietClose(ResultSet resultSet) {
        try {
            if (resultSet != null)
                resultSet.close();
        } catch (SQLException e) {
            logger.warn("Couldn't close {}", resultSet, e);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    public static SQLException chainSQLException(SQLException main, SQLException next) {
        if (main == null)
            return next;
        main.setNextException(next);
        return main;
    }
}
