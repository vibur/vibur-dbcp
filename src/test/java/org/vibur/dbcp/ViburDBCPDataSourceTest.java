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
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import org.vibur.dbcp.cache.ConnMethodKey;
import org.vibur.dbcp.cache.StatementVal;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.vibur.dbcp.cache.StatementVal.AVAILABLE;
import static org.vibur.dbcp.cache.StatementVal.EVICTED;
import static org.vibur.dbcp.util.StatementCacheUtils.mockStatementCache;

/**
 * JDBC integration tests.
 *
 * @author Simeon Malchev
 */
@RunWith(MockitoJUnitRunner.class)
public class ViburDBCPDataSourceTest extends AbstractDataSourceTest {

    @Captor
    private ArgumentCaptor<ConnMethodKey> key1, key2;
    @Captor
    private ArgumentCaptor<StatementVal> val1, val2;

    @Test
    public void testSelectStatementNoStatementsCache() throws SQLException, IOException {
        DataSource ds = createDataSourceNoStatementsCache();
        doTestSelectStatement(ds);
    }

    @Test
    public void testSelectStatementFromExternalDataSource() throws SQLException, IOException {
        DataSource ds = createDataSourceFromExternalDataSource();
        doTestSelectStatement(ds);
    }

    @Test
    public void testSelectStatementWithStatementsCache() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            ConcurrentMap<ConnMethodKey, StatementVal> mockedStatementCache = mockStatementCache(ds);

            connection = ds.getConnection();
            executeAndVerifySelectStatement(connection);
            executeAndVerifySelectStatement(connection);

            verifyZeroInteractions(mockedStatementCache);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testPreparedSelectStatementNoStatementsCache() throws SQLException, IOException {
        DataSource ds = createDataSourceNoStatementsCache();
        doTestPreparedSelectStatement(ds);
    }

    @Test
    public void testPreparedSelectStatementFromExternalDataSource() throws SQLException, IOException {
        DataSource ds = createDataSourceFromExternalDataSource();
        doTestPreparedSelectStatement(ds);
    }

    @Test
    public void testPreparedSelectStatementWithStatementsCache() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            ConcurrentMap<ConnMethodKey, StatementVal> mockedStatementCache = mockStatementCache(ds);

            connection = ds.getConnection();
            executeAndVerifyPreparedSelectStatement(connection);
            executeAndVerifyPreparedSelectStatement(connection);

            InOrder inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), val1.capture());
            inOrder.verify(mockedStatementCache).get(key2.capture());

            assertEquals(1, mockedStatementCache.size());
            assertTrue(mockedStatementCache.containsKey(key1.getValue()));
            assertEquals(key1.getValue(), key2.getValue());
            assertEquals("prepareStatement", key1.getValue().getMethod().getName());
            assertEquals(AVAILABLE, val1.getValue().state().get());
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testTwoPreparedSelectStatementsWithStatementsCache() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceWithStatementsCache();
        Connection connection = null;
        try {
            ConcurrentMap<ConnMethodKey, StatementVal> mockedStatementCache = mockStatementCache(ds);

            connection = ds.getConnection();
            executeAndVerifyPreparedSelectStatement(connection);
            executeAndVerifyPreparedSelectStatementByLastName(connection);

            InOrder inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), val1.capture());
            inOrder.verify(mockedStatementCache).get(key2.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key2.getValue()), val2.capture());

            // key1 will be evicted from the StatementCache because its capacity set to 1.
            assertEquals(1, mockedStatementCache.size());
            assertTrue(mockedStatementCache.containsKey(key2.getValue()));
            assertNotEquals(key1.getValue(), key2.getValue());
            assertEquals("prepareStatement", key1.getValue().getMethod().getName());
            assertEquals("prepareStatement", key2.getValue().getMethod().getName());
            assertTrue(val1.getValue().state().get() == EVICTED);
            assertTrue(val2.getValue().state().get() == AVAILABLE);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testExceptionOnOneConnectionDoesNotImpactOtherConnections() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();
        assertEquals(POOL_INITIAL_SIZE, ds.getPool().remainingCreated());

        // Executing a Statement that will produce an SQLException:
        Connection connection = ds.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop table nonexistent");
            fail("SQLException expected");
        } catch (SQLException ignored) {
            // no-op
        } finally {
            connection.close();
        }
        Connection internal1 = connection.unwrap(Connection.class);
        assertTrue(internal1.isClosed());
        assertEquals(POOL_INITIAL_SIZE - 1, ds.getPool().remainingCreated()); // the remainingCreated connections count should decrease by 1

        // Executing a Statement that will not cause an exception:
        connection = ds.getConnection();
        try {
            executeAndVerifySelectStatement(connection);
        } finally {
            connection.close();
        }
        Connection internal2 = connection.unwrap(Connection.class);
        assertNotSame(internal1, internal2);
        assertFalse(internal2.isClosed());
        assertEquals(POOL_INITIAL_SIZE - 1, ds.getPool().remainingCreated()); // the remainingCreated connections count should not decrease more
    }

    @Test
    public void testStatementCloseShouldCloseTheInternalStatementToo() throws SQLException, IOException {
        DataSource ds = createDataSourceNoStatementsCache();

        Connection connection = ds.getConnection();
        Statement statement = connection.createStatement();
        PreparedStatement pStatement = connection.prepareStatement("select * from actor where first_name = ?");

        pStatement.close();
        statement.close();
        connection.close();

        Statement internal1 = statement.unwrap(Statement.class);
        assertTrue(internal1.isClosed());
        PreparedStatement internal2 = pStatement.unwrap(PreparedStatement.class);
        assertTrue(internal2.isClosed());
    }

    @Test
    public void testConnectionCloseAfterPoolTerminationShouldCloseTheInternalConnectionToo() throws SQLException, IOException {
        ViburDBCPDataSource ds = createDataSourceNoStatementsCache();

        Connection connection = ds.getConnection();
        ds.terminate();
        connection.close();

        Connection internal1 = connection.unwrap(Connection.class);
        assertTrue(internal1.isClosed());
    }

    private void doTestSelectStatement(DataSource ds) throws SQLException {
        Connection connection = ds.getConnection();
        try {
            executeAndVerifySelectStatement(connection);
        } finally {
            connection.close();
        }
        assertTrue(connection.isClosed());
    }

    private void doTestPreparedSelectStatement(DataSource ds) throws SQLException {
        Connection connection = ds.getConnection();
        try {
            executeAndVerifyPreparedSelectStatement(connection);
        } finally {
            connection.close();
        }
        assertTrue(connection.isClosed());
    }

    private void executeAndVerifySelectStatement(Connection connection) throws SQLException {
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
        assertTrue(resultSet.isClosed());
        assertTrue(statement.isClosed());
    }

    private void executeAndVerifyPreparedSelectStatement(Connection connection) throws SQLException {
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
        assertTrue(resultSet.isClosed());
        assertTrue(pStatement.isClosed());
    }

    private void executeAndVerifyPreparedSelectStatementByLastName(Connection connection) throws SQLException {
        PreparedStatement pStatement = null;
        ResultSet resultSet = null;
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            pStatement = connection.prepareStatement("select * from actor where last_name = ?");
            pStatement.setString(1, "CROWE");
            resultSet = pStatement.executeQuery();

            Set<String> firstNames = new HashSet<String>(Collections.singletonList("SIDNEY"));
            int count = 0;
            while (resultSet.next()) {
                ++count;
                String firstName = resultSet.getString("first_name");
                assertTrue(firstNames.remove(firstName));
            }
            assertEquals(1, count);
        } finally {
            if (resultSet != null) resultSet.close();
            if (pStatement != null) pStatement.close();
        }
        assertTrue(resultSet.isClosed());
        assertTrue(pStatement.isClosed());
    }
}
