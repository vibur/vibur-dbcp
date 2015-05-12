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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Simeon Malchev
 */
public class WrapperTest extends AbstractDataSourceTest {

    @Test
    public void testWrapperMethods() throws SQLException, IOException {
        DataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        Statement statement = null;
        PreparedStatement pStatement = null;
        CallableStatement cStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ds.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            pStatement = connection.prepareStatement("select count(*) from actor");
            cStatement = connection.prepareCall("select count(*) from actor");
            DatabaseMetaData metaData = connection.getMetaData();

            assertTrue(metaData.isWrapperFor(DatabaseMetaData.class));
            DatabaseMetaData md = metaData.unwrap(DatabaseMetaData.class);
            assertNotNull(md);
            assertNotEquals(md, metaData);

            assertTrue(cStatement.isWrapperFor(CallableStatement.class));
            CallableStatement cs = cStatement.unwrap(CallableStatement.class);
            assertNotNull(cs);
            assertNotEquals(cs, cStatement);

            assertTrue(pStatement.isWrapperFor(PreparedStatement.class));
            PreparedStatement ps = pStatement.unwrap(PreparedStatement.class);
            assertNotNull(ps);
            assertNotEquals(ps, pStatement);

            assertTrue(statement.isWrapperFor(Statement.class));
            Statement s = statement.unwrap(Statement.class);
            assertNotNull(s);
            assertNotEquals(s, statement);

            assertTrue(resultSet.isWrapperFor(ResultSet.class));
            ResultSet r = resultSet.unwrap(ResultSet.class);
            assertNotNull(r);
            assertNotEquals(r, resultSet);

            assertTrue(connection.isWrapperFor(Connection.class));
            Connection c = connection.unwrap(Connection.class);
            assertNotNull(c);
            assertNotEquals(c, connection);
        } finally {
            if (resultSet != null) resultSet.close();
            if (cStatement != null) cStatement.close();
            if (pStatement != null) pStatement.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }
}
