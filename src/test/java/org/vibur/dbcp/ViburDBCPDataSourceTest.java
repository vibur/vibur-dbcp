/**
 * Copyright 2013-2025 Simeon Malchev
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.vibur.dbcp.stcache.StatementHolder;
import org.vibur.dbcp.stcache.StatementMethod;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_INTERRUPTED_ERROR;
import static org.vibur.dbcp.stcache.StatementHolder.State.AVAILABLE;
import static org.vibur.dbcp.stcache.StatementHolder.State.EVICTED;

/**
 * JDBC integration tests.
 *
 * @author Simeon Malchev
 */
@ExtendWith(MockitoExtension.class)
public class ViburDBCPDataSourceTest extends AbstractDataSourceTest {

    @Test
    public void testSelectStatementNoStatementsCache() throws SQLException {
        DataSource ds = createDataSourceNoStatementsCache();
        doTestSelectStatement(ds);
    }

    @Test
    public void testSelectStatementFromExternalDataSource() throws SQLException {
        DataSource ds = createDataSourceFromExternalDataSource();
        doTestSelectStatement(ds);
    }

    @Test
    public void testSelectStatementWithStatementsCache() throws SQLException {
        var ds = createDataSourceWithStatementsCache();
        var mockedStatementCache = mockStatementCache(ds);

        try (var connection = ds.getConnection()) {
            executeAndVerifySelectStatement(connection);
            executeAndVerifySelectStatement(connection);

            verifyNoInteractions(mockedStatementCache);
        }
    }

    @Test
    public void testPreparedSelectStatementNoStatementsCache() throws SQLException {
        DataSource ds = createDataSourceNoStatementsCache();
        doTestPreparedSelectStatement(ds);
    }

    @Test
    public void testPreparedSelectStatementFromExternalDataSource() throws SQLException {
        DataSource ds = createDataSourceFromExternalDataSource();
        doTestPreparedSelectStatement(ds);
    }

    @Test
    public void testPreparedSelectStatementWithStatementsCache() throws SQLException {
        var key1 = ArgumentCaptor.forClass(StatementMethod.class);
        var key2 = ArgumentCaptor.forClass(StatementMethod.class);
        var val1 = ArgumentCaptor.forClass(StatementHolder.class);

        var ds = createDataSourceWithStatementsCache();
        var mockedStatementCache = mockStatementCache(ds);

        try (var connection = ds.getConnection()) {
            executeAndVerifyPreparedSelectStatement(connection);
            executeAndVerifyPreparedSelectStatement(connection);

            var inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), val1.capture());
            inOrder.verify(mockedStatementCache).get(key2.capture());

            assertEquals(1, mockedStatementCache.size());
            assertTrue(mockedStatementCache.containsKey(key1.getValue()));
            assertEquals(key1.getValue(), key2.getValue());
            assertEquals(AVAILABLE, val1.getValue().state().get());
        }
    }

    @Test
    public void testTwoPreparedSelectStatementsWithStatementsCache() throws SQLException {
        var key1 = ArgumentCaptor.forClass(StatementMethod.class);
        var key2 = ArgumentCaptor.forClass(StatementMethod.class);
        var val1 = ArgumentCaptor.forClass(StatementHolder.class);
        var val2 = ArgumentCaptor.forClass(StatementHolder.class);

        var ds = createDataSourceWithStatementsCache();
        var mockedStatementCache = mockStatementCache(ds);

        try (var connection = ds.getConnection()) {
            executeAndVerifyPreparedSelectStatement(connection);
            executeAndVerifyPreparedSelectStatementByLastName(connection);

            var inOrder = inOrder(mockedStatementCache);
            inOrder.verify(mockedStatementCache).get(key1.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), val1.capture());
            inOrder.verify(mockedStatementCache).get(key2.capture());
            inOrder.verify(mockedStatementCache).putIfAbsent(same(key2.getValue()), val2.capture());

            // key1 will be evicted from the StatementCache because its capacity is set to 1.
            assertEquals(1, mockedStatementCache.size());
            assertTrue(mockedStatementCache.containsKey(key2.getValue()));
            assertNotEquals(key1.getValue(), key2.getValue());
            assertEquals(EVICTED, val1.getValue().state().get());
            assertEquals(AVAILABLE, val2.getValue().state().get());
        }
    }

    @Test
    public void testExceptionOnOneConnectionDoesNotImpactOtherConnections() throws SQLException {
        var ds = createDataSourceNoStatementsCache();
        assertEquals(POOL_INITIAL_SIZE, ds.getPool().remainingCreated());

        // Executing a Statement that will produce an SQLException:
        var connection = ds.getConnection();
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("drop table nonexistent");
            fail("SQLException expected");
        } catch (SQLException ignored) {
            // no-op
        } finally {
            connection.close();
        }
        var internalConn1 = connection.unwrap(Connection.class);
        assertTrue(internalConn1.isClosed());
        assertEquals(POOL_INITIAL_SIZE - 1, ds.getPool().remainingCreated()); // the remainingCreated connections count should decrease by 1

        // Executing a Statement that will not cause an exception:
        connection = ds.getConnection();
        try {
            executeAndVerifySelectStatement(connection);
        } finally {
            connection.close();
        }
        var internalConn2 = connection.unwrap(Connection.class);
        assertNotSame(internalConn1, internalConn2);
        assertFalse(internalConn2.isClosed());
        assertEquals(POOL_INITIAL_SIZE - 1, ds.getPool().remainingCreated()); // the remainingCreated connections count should not decrease more
    }

    @Test
    public void testStatementCloseShouldCloseTheInternalStatementToo() throws SQLException {
        DataSource ds = createDataSourceNoStatementsCache();

        var connection = ds.getConnection();
        var statement = connection.createStatement();
        var pStatement = connection.prepareStatement("select * from actor where first_name = ?");

        pStatement.close();
        statement.close();
        connection.close();

        var internalStatement = statement.unwrap(Statement.class);
        assertTrue(statement.isClosed());
        assertTrue(internalStatement.isClosed());

        var internalPStatement = pStatement.unwrap(PreparedStatement.class);
        assertTrue(pStatement.isClosed());
        assertTrue(internalPStatement.isClosed());
    }

    @Test
    public void testConnectionCloseAfterPoolTerminationShouldCloseTheInternalConnectionToo() throws SQLException {
        var ds = createDataSourceWithTracking();

        var connection = ds.getConnection();
        ds.close();

        var internalConn = connection.unwrap(Connection.class);
        assertFalse(connection.isClosed());
        assertFalse(internalConn.isClosed());

        connection.close();
        assertTrue(connection.isClosed());
        assertTrue(internalConn.isClosed());
    }

    @Test
    public void testGetConnectionAfterPoolTermination() throws SQLException {
        var ds = createDataSourceNoStatementsCache();
        ds.setAllowConnectionAfterTermination(true); // enable the feature
        ds.close();

        var connection = ds.getConnection();
        assertFalse(connection.isClosed());
        assertFalse(Proxy.isProxyClass(connection.getClass())); // i.e., that is a native Connection
        connection.close();
    }

    @Test
    public void testGetConnectionAfterPoolTerminationFail() {
        var ds = createDataSourceNoStatementsCache();
        ds.setAllowConnectionAfterTermination(false); // that's the default value
        ds.close();

        assertThrows(SQLException.class, ds::getConnection);
    }

    @Test
    public void testGetNonPooledConnection() throws SQLException {
        var ds = createDataSourceNoStatementsCache();
        var connection = ds.getNonPooledConnection();
        assertFalse(connection.isClosed());
        assertFalse(Proxy.isProxyClass(connection.getClass())); // i.e., that is a native Connection
        connection.close();
    }

    @Test
    public void testSeverPooledConnection() throws SQLException {
        var ds = createDataSourceNoStatementsCache();
        var connection = ds.getConnection();
        var internalConn = connection.unwrap(Connection.class);
        var createdTotal = ds.getPool().createdTotal();

        assertFalse(connection.isClosed());
        assertFalse(internalConn.isClosed());
        ds.severConnection(connection); // severing the proxy connection closes the underlying raw connection, too
        assertTrue(connection.isClosed());
        assertTrue(internalConn.isClosed());
        assertEquals(createdTotal - 1, ds.getPool().createdTotal()); // the remainingCreated connections count should decrease by 1
    }

    @Test
    public void testSeverNonPooledConnection() throws SQLException {
        var ds = createDataSourceNoStatementsCache();
        var connection = ds.getNonPooledConnection();

        assertFalse(connection.isClosed());
        ds.severConnection(connection);
        assertTrue(connection.isClosed());
    }

    @Test
    public void testTakenConnections() throws SQLException {
        var ds = createDataSourceWithTracking();
        var connection = ds.getConnection();

        var takenConnections1 = ds.getTakenConnections();
        assertEquals(1, takenConnections1.length);
        assertSame(connection, takenConnections1[0].getProxyConnection());

        var currentNanoTime = System.nanoTime();
        var takenNanoTime = takenConnections1[0].getTakenNanoTime();
        assertTrue(takenNanoTime > 0);
        assertTrue(currentNanoTime > takenNanoTime);
        assertEquals(0, takenConnections1[0].getLastAccessNanoTime());

        var takenConnections2 = ds.getTakenConnections();
        assertEquals(1, takenConnections2.length);
        assertNotSame(takenConnections1, takenConnections2);
        assertNotSame(takenConnections1[0], takenConnections2[0]);
        assertSame(takenConnections1[0].getProxyConnection(), takenConnections2[0].getProxyConnection());

        ds.close();

        var takenConnections3 = ds.getTakenConnections(); // verify that TakenConnections can be obtained after the pool was closed
        assertEquals(1, takenConnections3.length);
        assertNotSame(takenConnections2, takenConnections3);
        assertNotSame(takenConnections2[0], takenConnections3[0]);
        assertSame(takenConnections2[0].getProxyConnection(), takenConnections3[0].getProxyConnection());

        connection.close();

        var takenConnections4 = ds.getTakenConnections();
        assertEquals(0, takenConnections4.length);
    }

    @Test
    public void testLogTakenConnectionsOnTimeout() throws SQLException {
        @SuppressWarnings("resource")
        var ds = createDataSourceNotStarted();

        ds.setPoolInitialSize(1);
        ds.setPoolMaxSize(2);
        ds.setConnectionTimeoutInMs(10);
        ds.setLogTakenConnectionsOnTimeout(true);
        // This regex filters out any lines that contain "mockito", "junit" or "reflect" substrings
        ds.setLogLineRegex(Pattern.compile("^((?!mockito|junit|reflect).)*$"));
        ds.start();

        try (var ignored1 = ds.getConnection();
             var ignored2 = ds.getConnection()) {

            assertThrows(SQLTimeoutException.class, ds::getConnection);
        }
    }

    @Test
    public void testInterruptedWhileGettingConnection() {
        @SuppressWarnings("resource")
        var ds = createDataSourceWithTracking();

        Thread.currentThread().interrupt();
        try {
            ds.getConnection();
            fail("SQLException expected");
        } catch (SQLException e) {
            assertEquals(SQLSTATE_INTERRUPTED_ERROR, e.getSQLState());
        } finally {
            assertTrue(Thread.interrupted()); // clears the interrupted flag in order to not affect subsequent tests
        }
    }

    private static void doTestSelectStatement(DataSource ds) throws SQLException {
        try (var connection = ds.getConnection()) {
            executeAndVerifySelectStatement(connection);
        }
    }

    private static void doTestPreparedSelectStatement(DataSource ds) throws SQLException {
        try (var connection = ds.getConnection()) {
            executeAndVerifyPreparedSelectStatement(connection);
        }
    }

    private static void executeAndVerifySelectStatement(Connection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            var resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            Set<String> expectedLastNames = new HashSet<>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
            while (resultSet.next()) {
                var lastName = resultSet.getString("last_name");
                assertTrue(expectedLastNames.remove(lastName));
            }
            assertTrue(expectedLastNames.isEmpty());
        }
    }

    private static void executeAndVerifyPreparedSelectStatement(Connection connection) throws SQLException {
        try (var pStatement = connection.prepareStatement("select * from actor where first_name = ?")) {
            pStatement.setString(1, "CHRISTIAN");
            var resultSet = pStatement.executeQuery();
            Set<String> expectedLastNames = new HashSet<>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
            while (resultSet.next()) {
                var lastName = resultSet.getString("last_name");
                assertTrue(expectedLastNames.remove(lastName));
            }
            assertTrue(expectedLastNames.isEmpty());
        }
    }

    private static void executeAndVerifyPreparedSelectStatementByLastName(Connection connection) throws SQLException {
        try (var pStatement = connection.prepareStatement("select * from actor where last_name = ?")) {
            pStatement.setString(1, "CROWE");
            var resultSet = pStatement.executeQuery();
            Set<String> expectedFirstNames = new HashSet<>(Collections.singletonList("SIDNEY"));
            while (resultSet.next()) {
                var firstName = resultSet.getString("first_name");
                assertTrue(expectedFirstNames.remove(firstName));
            }
            assertTrue(expectedFirstNames.isEmpty());
        }
    }
}
