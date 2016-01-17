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

package org.vibur.dbcp.restriction;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.matchers.Contains;
import org.vibur.dbcp.AbstractDataSourceTest;
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.util.AbstractConnectionRestriction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vibur.dbcp.restriction.QueryRestrictions.BLACKLISTED_DML;
import static org.vibur.dbcp.restriction.QueryRestrictions.WHITELISTED_DML;

/**
 * Restricted connection tests.
 *
 * @author Simeon Malchev
 */
public class RestrictedConnectionTest extends AbstractDataSourceTest {

    private static final String RESTRICTED_ERR_STR = "with a restricted SQL query";
    private static final Matcher<String> RESTRICTED_ERR = new Contains(RESTRICTED_ERR_STR);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final ConnectionRestriction whiteListedDml = new AbstractConnectionRestriction() {
        @Override
        public QueryRestriction getQueryRestriction() {
            return WHITELISTED_DML;
        }
    };
    private final ConnectionRestriction blackListedDml = new AbstractConnectionRestriction() {
        @Override
        public QueryRestriction getQueryRestriction() {
            return BLACKLISTED_DML;
        }
    };

    @Test
    public void testAllowedExecute() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        Connection restrictedConn = null;
        try {
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();
            try {
                executeAndVerifySelectStatement(restrictedConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
                fail("SQLException expected");
            } catch (SQLException ignored) {
                assertTrue(ignored.getMessage().contains(RESTRICTED_ERR_STR));
            }

            ds.setConnectionRestriction(null);
            freeConn = ds.getConnection();
            executeAndVerifySelectStatement(freeConn, " SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass
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
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();
            try {
                restrictedPStatement = restrictedConn.prepareStatement(" select * from actor where first_name = ?"); // will throw SQLException
                fail("SQLException expected");
            } catch (SQLException ignored) {
                assertTrue(ignored.getMessage().contains(RESTRICTED_ERR_STR));
            }

            ds.setConnectionRestriction(null);
            freeConn = ds.getConnection();
            freePStatement = freeConn.prepareStatement(" select * from actor where first_name = ?"); // will pass
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
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();
            restrictedStatement = restrictedConn.createStatement();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
            ds.setConnectionRestriction(blackListedDml);
            restrictedConn = ds.getConnection();
            restrictedStatement = restrictedConn.createStatement();
            try {
                restrictedStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will throw SQLException
                fail("SQLException expected");
            } catch (SQLException ignored) {
                assertTrue(ignored.getMessage().contains(RESTRICTED_ERR_STR));
            }

            ds.setConnectionRestriction(null);
            freeConn = ds.getConnection();
            freeStatement = freeConn.createStatement();
            freeStatement.addBatch("SELECT * from actor where first_name = 'CHRISTIAN'"); // will pass
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
            ds.setConnectionRestriction(whiteListedDml);
            restrictedConn = ds.getConnection();
            restrictedStatement = restrictedConn.createStatement();

            exception.expect(SQLException.class);
            exception.expectMessage(RESTRICTED_ERR);
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
