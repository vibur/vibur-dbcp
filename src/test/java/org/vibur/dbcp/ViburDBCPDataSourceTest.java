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
        Connection connection = null;
        try {
            connection = ds.getConnection();
            executeAndVerifySelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testSelectStatementFromExternalDataSource() throws SQLException, IOException {
        DataSource ds = createDataSourceFromExternalDataSource();
        Connection connection = null;
        try {
            connection = ds.getConnection();
            executeAndVerifySelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    @SuppressWarnings("unchecked")
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
        Connection connection = null;
        try {
            connection = ds.getConnection();
            executeAndVerifyPreparedSelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testPreparedSelectStatementFromExternalDataSource() throws SQLException, IOException {
        DataSource ds = createDataSourceFromExternalDataSource();
        Connection connection = null;
        try {
            connection = ds.getConnection();
            executeAndVerifyPreparedSelectStatement(connection);
        } finally {
            if (connection != null) connection.close();
        }
        assertNotNull(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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
        assertNotNull(connection);
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
        assertNotNull(connection);
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
        assertTrue(pStatement.isClosed());
    }
}
