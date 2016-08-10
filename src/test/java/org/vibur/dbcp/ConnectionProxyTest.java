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
        try (Connection connection = ds.getConnection();
            Statement statement = connection.createStatement();
            PreparedStatement pStatement = connection.prepareStatement("select count(*) from actor");
            CallableStatement cStatement = connection.prepareCall("select count(*) from actor")) {

            assertSame(connection, statement.getConnection());
            assertSame(connection, pStatement.getConnection());
            assertSame(connection, cStatement.getConnection());
            DatabaseMetaData metaData = connection.getMetaData();
            assertSame(connection, metaData.getConnection());
        }
    }
}
