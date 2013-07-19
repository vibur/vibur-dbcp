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

package vibur.dbcp;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import vibur.dbcp.common.IntegrationTest;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Prerequisites for running the tests:
 *
 * <p>1. Install and run MySQL server.
 *
 * <p>2. Install the <a href="http://dev.mysql.com/doc/sakila/en/">Sakila Sample Database</a>
 * as described in the link.
 *
 * <p>3. The following system properties have to be provided and set to something similar to:
 * <p>
 * -DDriverClassName=com.mysql.jdbc.Driver <br>
 * -DJdbcUrl=jdbc:mysql://localhost/sakila <br>
 * -DUsername=USERNAME <br>
 * -DPassword=PASSWORD <br>
 *
 * @author Simeon Malchev
 */
@Category({IntegrationTest.class})
public class ViburDBCPDataSourceTest {

    private ViburDBCPDataSource viburDS = null;

    @After
    public void tearDown() throws Exception {
        if (viburDS != null) {
            viburDS.terminate();
            viburDS= null;
        }
    }

    @Test
    public void testSimpleStatementSelectNoStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeSimpleStatementSelect(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testSimpleStatementSelectWithStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeSimpleStatementSelect(connection);
            executeSimpleStatementSelect(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    private void executeSimpleStatementSelect(Connection connection) throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");

            Set<String> expectedLastNames = new HashSet<String>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
            int count = 0;
            while (resultSet.next()) {
                ++count;
                String lastName = resultSet.getString("last_name");
                assertTrue(expectedLastNames.remove(lastName));
            }
            assertEquals(3, count);
        } finally {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
        }
    }

    @Test
    public void testSimplePreparedStatementSelectNoStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeSimplePreparedStatementSelect(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testSimplePreparedStatementSelectWithStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeSimplePreparedStatementSelect(connection);
            executeSimplePreparedStatementSelect(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    private void executeSimplePreparedStatementSelect(Connection connection) throws SQLException {
        PreparedStatement pStatement = null;
        ResultSet resultSet = null;
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            pStatement = connection.prepareStatement("select * from actor where first_name = ?");
            pStatement.setString(1, "CHRISTIAN");
            resultSet = pStatement.executeQuery();

            Set<String> lastNames = new HashSet<String>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
            int count = 0;
            while (resultSet.next()) {
                ++count;
                String lastName = resultSet.getString("last_name");
                assertTrue(lastNames.remove(lastName));
            }
            assertEquals(3, count);
        } finally {
            if (resultSet != null) resultSet.close();
            if (pStatement != null) pStatement.close();
        }
    }

    @Test
    public void testInitialiseFromPropertiesFile() {
        viburDS = new ViburDBCPDataSource("vibur-dbcp-test.properties");
    }

    @Test
    public void testInitialiseFromXMLPropertiesFile() {
        viburDS = new ViburDBCPDataSource("vibur-dbcp-test.xml");
        viburDS.start();
    }

    private DataSource getSimpleDataSourceNoStatementsCache() {
        viburDS = new ViburDBCPDataSource();

        viburDS.setDriverClassName(System.getProperty("DriverClassName"));
        viburDS.setJdbcUrl(System.getProperty("JdbcUrl"));
        viburDS.setUsername(System.getProperty("Username"));
        viburDS.setPassword(System.getProperty("Password"));

        viburDS.setPoolInitialSize(2);
        viburDS.setValidateOnRestore(true);

        viburDS.setLogStatementsEnabled(true);
        viburDS.setQueryExecuteTimeLimitInMs(1);

        viburDS.start();

        return viburDS;
    }

    private DataSource getSimpleDataSourceWithStatementsCache() {
        viburDS = new ViburDBCPDataSource();

        viburDS.setDriverClassName(System.getProperty("DriverClassName"));
        viburDS.setJdbcUrl(System.getProperty("JdbcUrl"));
        viburDS.setUsername(System.getProperty("Username"));
        viburDS.setPassword(System.getProperty("Password"));

        viburDS.setPoolInitialSize(2);
        viburDS.setValidateOnRestore(true);

        viburDS.setLogStatementsEnabled(true);
        viburDS.setQueryExecuteTimeLimitInMs(1);

        viburDS.setStatementCacheMaxSize(10);

        viburDS.start();

        return viburDS;
    }
}
