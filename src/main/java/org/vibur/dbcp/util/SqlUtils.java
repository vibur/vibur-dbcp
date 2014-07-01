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

import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * @author Simeon Malchev
 */
public final class SqlUtils {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SqlUtils.class);

    private SqlUtils() {}

    public static void closeStatement(Statement statement) {
        try {
            statement.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close {}", statement);
        }
    }

    public static void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.debug("Couldn't close {}", connection);
        }
    }

    public static String toSQLString(Statement statement, Object[] args, List<Object[]> executeParams) {
        StringBuilder result = new StringBuilder("-- ").append(statement instanceof PreparedStatement ?
                statement.toString() : Arrays.toString(args)); // the latter is for simple JDBC Statements
        if (!executeParams.isEmpty())
            result.append("\n-- Parameters:\n-- ").append(Arrays.deepToString(executeParams.toArray()));
        return result.toString();
    }
}
