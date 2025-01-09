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

import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;
import static org.vibur.dbcp.util.JdbcUtils.quietClose;

/**
 * @author Simeon Malchev
 */
public class HsqldbUtils {

    private static final String HSQLDB_SCHEMA_AND_DATA_SQL = "hsqldb_schema_and_data.sql";

    public static void deployDatabaseSchemaAndData(String jdbcUrl, String username, String password)
        throws IOException, SqlToolError, SQLException {

        Connection connection = null;
        try (var inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(HSQLDB_SCHEMA_AND_DATA_SQL)) {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            var isr = new InputStreamReader(requireNonNull(inputStream), Charset.defaultCharset().displayName());
            var sqlFile = new SqlFile(isr, "--sql", System.out, null, false, (URL) null);
            sqlFile.setConnection(connection);
            sqlFile.execute();
        } finally {
            quietClose(connection);
        }
    }
}
