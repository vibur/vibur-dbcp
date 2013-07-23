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
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import vibur.dbcp.cache.StatementKey;
import vibur.dbcp.cache.ValueHolder;
import vibur.dbcp.common.IntegrationTest;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalAnswers.*;

/**
 * Simple JDBC integration test. Prerequisites for running the tests:
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
@RunWith(MockitoJUnitRunner.class)
public class ViburDBCPDataSourceTest {

    private ViburDBCPDataSource viburDS = null;

    @Captor
    private ArgumentCaptor<StatementKey> key1, key2;

    @After
    public void tearDown() throws Exception {
        if (viburDS != null) {
            viburDS.terminate();
            viburDS= null;
        }
    }

    @Test
    public void testSimpleSelectStatementNoStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeAndVerifySimpleSelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleSelectStatementWithStatementsCache() throws SQLException {
        ViburDBCPDataSource ds = getSimpleDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            ConcurrentMap<StatementKey, ValueHolder<Statement>> mockedStatementCache =
                mock(ConcurrentMap.class, delegatesTo(ds.getStatementCache()));
            ds.setStatementCache(mockedStatementCache);

            connection = ds.getConnection();
            executeAndVerifySimpleSelectStatement(connection);
            executeAndVerifySimpleSelectStatement(connection);

            InOrder inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), any(ValueHolder.class));
            inOrder.verify(mockedStatementCache).get(key2.capture());

            assertEquals(key1.getValue(), key2.getValue());
            assertEquals("createStatement", key1.getValue().getMethod().getName());
            ValueHolder<Statement> valueHolder = mockedStatementCache.get(key1.getValue());
            assertFalse(valueHolder.inUse().get());
        } finally {
            if (connection != null) connection.close();
        }
    }

    private void executeAndVerifySimpleSelectStatement(Connection connection) throws SQLException {
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
    public void testSimplePreparedSelectStatementNoStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSourceNoStatementsCache();
        Connection connection = null;
        try {
            connection = ds.getConnection();

            executeAndVerifySimplePreparedSelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePreparedSelectStatementWithStatementsCache() throws SQLException {
        ViburDBCPDataSource ds = getSimpleDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            ConcurrentMap<StatementKey, ValueHolder<Statement>> mockedStatementCache =
                mock(ConcurrentMap.class, delegatesTo(ds.getStatementCache()));
            ds.setStatementCache(mockedStatementCache);

            connection = ds.getConnection();
            executeAndVerifySimplePreparedSelectStatement(connection);
            executeAndVerifySimplePreparedSelectStatement(connection);

            InOrder inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), any(ValueHolder.class));
            inOrder.verify(mockedStatementCache).get(key2.capture());

            assertEquals(key1.getValue(), key2.getValue());
            assertEquals("prepareStatement", key1.getValue().getMethod().getName());
            ValueHolder<Statement> valueHolder = mockedStatementCache.get(key1.getValue());
            assertFalse(valueHolder.inUse().get());
        } finally {
            if (connection != null) connection.close();
        }
    }

    private void executeAndVerifySimplePreparedSelectStatement(Connection connection) throws SQLException {
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
        assertEquals(2, viburDS.getPoolInitialSize());
    }

    @Test
    public void testInitialiseFromXMLPropertiesFile() {
        viburDS = new ViburDBCPDataSource("vibur-dbcp-test.xml");
        viburDS.start();
        assertEquals(2, viburDS.getPoolInitialSize());
    }

    private ViburDBCPDataSource getSimpleDataSourceNoStatementsCache() {
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

    private ViburDBCPDataSource getSimpleDataSourceWithStatementsCache() {
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
