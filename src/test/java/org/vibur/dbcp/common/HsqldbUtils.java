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

package org.vibur.dbcp.common;

import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPException;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Simeon Malchev
 */
public class HsqldbUtils {

    private static final Logger logger = LoggerFactory.getLogger(HsqldbUtils.class);

    private static final String HSQLDB_SCHEMA_AND_DATA_SQL = "src/test/resources/hsqldb_schema_and_data.sql";

    public static void deployDatabaseSchemaAndData(String driverClassName, String jdbcUrl, String username,
                                                   String password) throws IOException, SqlToolError {
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            throw new ViburDBCPException(e);
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            SqlFile sqlFile = new SqlFile(new File(HSQLDB_SCHEMA_AND_DATA_SQL));
            sqlFile.setConnection(connection);
            sqlFile.execute();
        } catch (SQLException e) {
            throw new ViburDBCPException(e);
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                logger.debug("Couldn't close " + connection, e);
            }
        }
    }
}
