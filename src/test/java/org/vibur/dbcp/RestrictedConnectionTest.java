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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.vibur.dbcp.ConnectionRestriction.*;

/**
 * Restricted connection tests.
 *
 * @author Simeon Malchev
 */
public class RestrictedConnectionTest extends AbstractDataSourceTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testAllowedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);
            executeAndVerifySelectStatement(connection, "SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testRestrictedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getRestrictedConnection(BLACKLISTED_DDL);

            thrown.expect(SQLException.class);
            executeAndVerifySelectStatement(connection, " SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testRestrictedAndUnrestrictedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        Connection freeConn = null;
        try {
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DDL);
            freeConn= ds.getConnection();

            executeAndVerifySelectStatement(freeConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass

            thrown.expect(SQLException.class);
            executeAndVerifySelectStatement(restrictedConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
        } finally {
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDdlExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);

            thrown.expect(SQLException.class);
            executeAndVerifySelectStatement(connection, "CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testAllowedPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        PreparedStatement pStatement = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);
            pStatement = connection.prepareStatement("select * from actor where first_name = ?");
        } finally {
            if (pStatement != null) pStatement.close();
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testRestrictedPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        PreparedStatement pStatement = null;
        try {
            connection = ds.getRestrictedConnection(BLACKLISTED_DDL);

            thrown.expect(SQLException.class);
            pStatement = connection.prepareStatement(" select * from actor where first_name = ?");
        } finally {
            if (pStatement != null) pStatement.close();
            if (connection != null) connection.close();
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
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DDL);
            freeConn= ds.getConnection();

            freePStatement = freeConn.prepareStatement(" select * from actor where first_name = ?"); // will pass

            thrown.expect(SQLException.class);
            restrictedPStatement = restrictedConn.prepareStatement(" select * from actor where first_name = ?"); // will throw SQLException
        } finally {
            if (freePStatement != null) freePStatement.close();
            if (restrictedPStatement != null) restrictedPStatement.close();
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDdlPrepare() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        PreparedStatement pStatement = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);

            thrown.expect(SQLException.class);
            pStatement = connection.prepareStatement("CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (pStatement != null) pStatement.close();
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testAllowedAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);
            statement = connection.createStatement();
            statement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testRestrictedAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ds.getRestrictedConnection(BLACKLISTED_DDL);
            statement = connection.createStatement();

            thrown.expect(SQLException.class);
            statement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'");
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
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
            restrictedConn = ds.getRestrictedConnection(BLACKLISTED_DDL);
            restrictedStatement = restrictedConn.createStatement();
            freeConn = ds.getConnection();
            freeStatement = freeConn.createStatement();

            freeStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass

            thrown.expect(SQLException.class);
            restrictedStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
        } finally {
            if (freeStatement != null) freeStatement.close();
            if (restrictedStatement != null) restrictedStatement.close();
            if (freeConn != null) freeConn.close();
            if (restrictedConn != null) restrictedConn.close();
        }
    }

    @Test
    public void testRestrictedDdlAddBatch() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ds.getRestrictedConnection(WHITELISTED_DDL);
            statement = connection.createStatement();

            thrown.expect(SQLException.class);
            statement.addBatch("CREATE TABLE new_actor(actor_id SMALLINT NOT NULL IDENTITY, first_name VARCHAR(45) NOT NULL)");
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
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
