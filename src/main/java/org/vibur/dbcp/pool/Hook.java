/**
 * Copyright 2016 Simeon Malchev
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

package org.vibur.dbcp.pool;

import org.vibur.dbcp.ViburConfig;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * An interface that holds all application programming hook interfaces for JDBC Connection tune-up, method
 * invocation interception, and more.
 * 
 * <p>These application hooks serve as extension points to the inner workings of the connection pool and have
 * access to the <b>raw (original)</b> JDBC Connection object, not the proxied such. In order to avoid interference
 * with how the connection pool manages its underlying connections, the application <b>must not</b> keep or store
 * in one or another form a reference to the {@code rawConnection} object.
 *
 * @see DefaultHook
 * @see ConnectionFactory
 * 
 * @author Simeon Malchev
 */
public interface Hook {

    ////////////////////
    // Connection hooks:

    interface InitConnection extends Hook {
        /**
         * A programming hook that will be invoked only once after the raw JDBC Connection is first created.
         * Its execution should take as short time as possible.
         *
         * <p>The {@code takenNanos} parameter includes as a minimum the time taken to establish the physical
         * connection to the database, plus possibly the time taken to make up to {@link ViburConfig#acquireRetryAttempts
         * that many} retry attempts that are separated by a {@link ViburConfig#acquireRetryDelayInMs retry delay}.
         *
         * @param rawConnection the just created raw JDBC connection
         * @param takenNanos the time taken to establish this connection in nanoseconds; also see above
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface ValidateConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection after it was taken from the pool and only
         * if it has stayed in the pool for longer than the predefined {@link ViburConfig#connectionIdleLimitInSeconds
         * idle} timeout. The invocation happens as part of the {@link javax.sql.DataSource#getConnection()
         * DataSource.getConnection()} flow. The hook execution should take as short time as possible.
         *
         * @param rawConnection the retrieved from the pool raw JDBC connection
         * @param idleNanos the time for which this connection has stayed in the pool in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred <i>or</i> that the given connection is <b>invalid</b>
         */
        void on(Connection rawConnection, long idleNanos) throws SQLException;
    }

    interface GetConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection after it was taken from the pool as part
         * of the {@link javax.sql.DataSource#getConnection() DataSource.getConnection()} flow.
         * Its execution should take as short time as possible.
         *
         * <p>Worth noting that the {@code takenNanos} parameter includes in the common case (and as a minimum) the time
         * taken to get the {@code rawConnection} from the pool, plus the {@link ValidateConnection#on time taken} to
         * validate the connection if validation was needed, or the {@link InitConnection#on time taken} to create
         * the connection if there was no ready connection in the pool but the pool capacity was not yet reached and
         * a new connection was lazily created upon this {@code getConnection()} request. For the last case, see also
         * the comments for the {@link ViburConfig#connectionTimeoutInMs connectionTimeoutInMs} configuration option.
         *
         * @param rawConnection the retrieved from the pool raw JDBC connection; <b>note that this can be {@code null}</b>
         *                      if we were unable to obtain a connection from the pool within the specified time limit
         * @param takenNanos the time taken to get this connection in nanoseconds; also see above
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface CloseConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection before it is restored back to the pool as
         * part of the {@link Connection#close()} flow. Its execution should take as short time as possible.
         *
         * @param rawConnection the raw JDBC connection that will be returned to the pool
         * @param takenNanos the time for which this connection was held by the application before it was restored
         *                   to the pool in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface DestroyConnection extends Hook {
        /**
         * A programming hook that will be invoked only once after the raw JDBC Connection is closed/destroyed.
         * Its execution should take as short time as possible.
         *
         * @param rawConnection the raw JDBC connection that was just closed
         * @param takenNanos the time taken to close/destroy this connection in nanoseconds
         */
        void on(Connection rawConnection, long takenNanos);
    }

    ////////////////////
    // Invocation hooks:

    interface MethodInvocation extends Hook {
        /**
         * An application hook that will be invoked before a method on any of the proxied JDBC interfaces is invoked.
         * Its execution should take as short time as possible.
         *
         * <p>For implementation details, see the comments for
         * {@link org.vibur.dbcp.proxy.InvocationHooksHolder#onMethodInvocation onMethodInvocation}.
         *
         * @param proxy the proxy instance that the method was invoked on
         * @param method the invoked method
         * @param args the method arguments
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Object proxy, Method method, Object[] args) throws SQLException;
    }

    interface StatementExecution extends Hook {
        /**
         * An application hook that will be invoked after each JDBC Statement "execute..." method call returns.
         * Its execution should take as short time as possible.
         *
         * @param sqlQuery the executed SQL query or prepared/callable SQL statement
         * @param queryParams the executed SQL query params if {@link ViburConfig#includeQueryParameters} is enabled,
         *                    {@code null} otherwise
         * @param takenNanos the time taken by the executed SQL query to complete in nanoseconds; also see the comments
         *                   for {@link ViburConfig#logQueryExecutionLongerThanMs}
         * @param sqlException an SQL exception that might have been thrown by the executed {@code sqlQuery};
         *                     {@code null} value means that no exception was thrown
         */
        void on(String sqlQuery, List<Object[]> queryParams, long takenNanos, SQLException sqlException);
    }

    interface ResultSetRetrieval extends Hook {
        /**
         * An application hook that will be invoked at the end of each ResultSet retrieval as part of the
         * {@link java.sql.ResultSet#close() ResultSet.close()} flow. For implementation details, see the comments for
         * {@link ViburConfig#logLargeResultSet}. Its execution should take as short time as possible.
         *
         * @param sqlQuery the executed SQL query or prepared/callable SQL statement
         * @param queryParams the executed SQL query params if {@link ViburConfig#includeQueryParameters} is enabled,
         *                    {@code null} otherwise
         * @param resultSetSize the retrieved ResultSet size
         */
        void on(String sqlQuery, List<Object[]> queryParams, long resultSetSize);
    }
}
