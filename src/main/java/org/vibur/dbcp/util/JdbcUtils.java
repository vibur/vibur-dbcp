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
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.util.hook.ConnectionHook;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Simeon Malchev
 */
public final class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {}

    public static void closeConnection(Connection rawConnection) {
        try {
            rawConnection.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close " + rawConnection, e);
        } catch (RuntimeException e) {
            logger.warn("Unexpected exception thrown by the JDBC driver for " + rawConnection, e);
        }
    }

    public static void closeStatement(Statement rawStatement) {
        try {
            rawStatement.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close " + rawStatement, e);
        } catch (RuntimeException e) {
            logger.warn("Unexpected exception thrown by the JDBC driver for " + rawStatement, e);
        }
    }

    public static void clearWarnings(Statement rawStatement) {
        try {
            rawStatement.clearWarnings();
        } catch (SQLException e) {
            logger.debug("Couldn't clearWarnings on " + rawStatement, e);
        } catch (RuntimeException e) {
            logger.warn("Unexpected exception thrown by the JDBC driver for " + rawStatement, e);
        }
    }

    public static void setDefaultValues(ViburDBCPConfig config, Connection rawConnection) throws SQLException {
        if (config.getDefaultAutoCommit() != null)
            rawConnection.setAutoCommit(config.getDefaultAutoCommit());
        if (config.getDefaultReadOnly() != null)
            rawConnection.setReadOnly(config.getDefaultReadOnly());
        if (config.getDefaultTransactionIsolationValue() != null)
            rawConnection.setTransactionIsolation(config.getDefaultTransactionIsolationValue());
        if (config.getDefaultCatalog() != null)
            rawConnection.setCatalog(config.getDefaultCatalog());
    }

    public static boolean validateConnection(Connection rawConnection, String query, int timeout,
                                             ConnectionHook validateConnectionHook) throws SQLException {
        if (query == null || query.trim().isEmpty())
            return true;

        if (validateConnectionHook != null)
            validateConnectionHook.on(rawConnection);

        if (query.equals(ViburDBCPConfig.IS_VALID_QUERY))
            return rawConnection.isValid(timeout);
        return executeQuery(rawConnection, query, timeout);
    }

    private static boolean executeQuery(Connection rawConnection, String query, int timeout) throws SQLException {
        Statement rawStatement = null;
        try {
            rawStatement = rawConnection.createStatement();
            rawStatement.setQueryTimeout(timeout);
            rawStatement.execute(query);
            return true;
        } finally {
            if (rawStatement != null)
                closeStatement(rawStatement);
        }
    }
}
