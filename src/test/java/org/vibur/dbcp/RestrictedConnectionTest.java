/**
 * Copyright 2015 Simeon Malchev
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

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.matchers.Contains;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.vibur.dbcp.ConnectionRestriction.BLACKLISTED_DML;
import static org.vibur.dbcp.ConnectionRestriction.WHITELISTED_DML;

/**
 * Restricted connection tests.
 *
 * @author Simeon Malchev
 */
public class RestrictedConnectionTest extends AbstractDataSourceTest {

    private static final Matcher<String> RESTRICTED_ERR_MESSAGE = new Contains("with a restricted SQL query");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testAllowedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);
            executeAndVerifySelectStatement(restrictedConn, "SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            executeAndVerifySelectStatement(restrictedConn, " SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedAndUnrestrictedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Connection freeConn = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);
            freeConn= ds.getConnection();

            executeAndVerifySelectStatement(freeConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            executeAndVerifySelectStatement(restrictedConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
        } finally {
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDDLExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            executeAndVerifySelectStatement(restrictedConn, "CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testAllowedPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        PreparedStatement restrictedPStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);
            restrictedPStatement = restrictedConn.prepareStatement("select * from actor where first_name = ?");
        } finally {
            if (restrictedPStatement != null) restrictedPStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        PreparedStatement restrictedPStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedPStatement = restrictedConn.prepareStatement(" select * from actor where first_name = ?");
        } finally {
            if (restrictedPStatement != null) restrictedPStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedAndUnrestrictedPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Connection freeConn = null;
        PreparedStatement restrictedPStatement = null;
        PreparedStatement freePStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);
            freeConn= ds.getConnection();

            freePStatement = freeConn.prepareStatement(" select * from actor where first_name = ?"); // will pass

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedPStatement = restrictedConn.prepareStatement(" select * from actor where first_name = ?"); // will throw SQLException
        } finally {
            if (freePStatement != null) freePStatement.close();
            if (restrictedPStatement != null) restrictedPStatement.close();
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDDLPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        PreparedStatement restrictedPStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedPStatement = restrictedConn.prepareStatement("CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (restrictedPStatement != null) restrictedPStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testAllowedAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Statement restrictedStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);
            restrictedStatement = restrictedConn.createStatement();
            restrictedStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (restrictedStatement != null) restrictedStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Statement restrictedStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);
            restrictedStatement = restrictedConn.createStatement();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (restrictedStatement != null) restrictedStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedAndUnrestrictedAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Connection freeConn = null;
        Statement restrictedStatement = null;
        Statement freeStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DML);
            restrictedStatement = restrictedConn.createStatement();
            freeConn = ds.getConnection();
            freeStatement = freeConn.createStatement();

            freeStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
        } finally {
            if (freeStatement != null) freeStatement.close();
            if (restrictedStatement != null) restrictedStatement.close();
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDDLAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Statement restrictedStatement = null;
        try {
            restrictedConn = ds.getRestrictedConnection(WHITELISTED_DML);
            restrictedStatement = restrictedConn.createStatement();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR_MESSAGE);
            restrictedStatement.addBatch("CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (restrictedStatement != null) restrictedStatement.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    private void executeAndVerifySelectStatement(Connection connection, String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } finally {
            if (statement != null) statement.close();
        }
    }
}
