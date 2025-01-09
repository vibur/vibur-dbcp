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

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Simeon Malchev
 */
public class WrapperTest extends AbstractDataSourceTest {

    @Test
    public void testWrapperMethods() throws SQLException {
        @SuppressWarnings("resource")
        DataSource ds = createDataSourceNoStatementsCache();

        try (var connection = ds.getConnection();
             var statement = connection.createStatement();
             var pStatement = connection.prepareStatement("select count(*) from actor");
             var cStatement = connection.prepareCall("select count(*) from actor")) {

            var metaData = connection.getMetaData();

            assertTrue(metaData.isWrapperFor(DatabaseMetaData.class));
            var md = metaData.unwrap(DatabaseMetaData.class);
            assertNotNull(md);
            assertNotEquals(md, metaData);

            assertTrue(cStatement.isWrapperFor(CallableStatement.class));
            var cs = cStatement.unwrap(CallableStatement.class);
            assertNotNull(cs);
            assertNotEquals(cs, cStatement);

            assertTrue(pStatement.isWrapperFor(PreparedStatement.class));
            var ps = pStatement.unwrap(PreparedStatement.class);
            assertNotNull(ps);
            assertNotEquals(ps, pStatement);

            assertTrue(statement.isWrapperFor(Statement.class));
            var s = statement.unwrap(Statement.class);
            assertNotNull(s);
            assertNotEquals(s, statement);

            var resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            assertTrue(resultSet.isWrapperFor(ResultSet.class));
            var r = resultSet.unwrap(ResultSet.class);
            assertNotNull(r);
            assertNotEquals(r, resultSet);

            assertTrue(connection.isWrapperFor(Connection.class));
            var c = connection.unwrap(Connection.class);
            assertNotNull(c);
            assertNotEquals(c, connection);
        }
    }
}
