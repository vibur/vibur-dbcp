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

import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.assertSame;

/**
 * @author Simeon Malchev
 */
public class ConnectionProxyTest extends AbstractDataSourceTest {

    @Test
    public void testSameConnection() throws SQLException, IOException {
        DataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        Statement statement = null;
        PreparedStatement pStatement = null;
        CallableStatement cStatement = null;
        try {
            connection = ds.getConnection();
            statement = connection.createStatement();
            pStatement = connection.prepareStatement("select count(*) from actor");
            cStatement = connection.prepareCall("select count(*) from actor");
            DatabaseMetaData metaData = connection.getMetaData();

            assertSame(connection, statement.getConnection());
            assertSame(connection, pStatement.getConnection());
            assertSame(connection, cStatement.getConnection());
            assertSame(connection, metaData.getConnection());
        } finally {
            if (cStatement != null) cStatement.close();
            if (pStatement != null) pStatement.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}
