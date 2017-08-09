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
import java.sql.Statement;
import java.util.List;

/**
 * An interface that holds all application programming hook interfaces for JDBC Connection tune-up, method
 * invocation interception, and more.
 * 
 * <p>These application hooks serve as extension points to the inner workings of the connection pool and some of them
 * have access to the <b>raw (original)</b> JDBC Connection object, not the proxied such. In order to avoid interference
 * with the way the connection pool manages its underlying connections, the application <b>must not</b> keep or store
 * in one or another form a reference to the {@code rawConnection} object.
 *
 * <p>Multiple hooks of one and the same type can be registered and they will be executed in the order in which they
 * were registered; i.e., if there are N registered hooks from a particular type, the first registered hook will
 * be executed first, then the second, the third, and so on.
 *
 * @see DefaultHook
 * @see ConnectionFactory
 * 
 * @author Simeon Malchev
 */
public interface Hook {

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Connection hooks:

    interface InitConnection extends Hook {
        /**
         * A programming hook that will be invoked only once <i>after</i> the raw JDBC Connection is first created.
         * Its execution should take as short time as possible.
         *
         * <p>The {@code takenNanos} parameter includes as a minimum the time taken to establish (or attempt to
         * establish) one physical connection to the database, plus possibly the time taken to make up to
         * {@link ViburConfig#acquireRetryAttempts} that are separated by a {@link ViburConfig#acquireRetryDelayInMs},
         * if there were errors while trying to establish the connection.
         *
         * <p>Worth noting that since version 19.0 the {@code rawConnection} parameter can be {@code null}, which
         * means that the attempt to establish a connection, plus all reattempts, were unsuccessful.
         *
         * @param rawConnection the just created <b>raw</b> JDBC connection; <b>note that it can be
         *                      {@code null}</b> if we were unable to establish a connection to the database
         * @param takenNanos the time taken to establish (or attempt to establish) the connection in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface GetConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection <i>after</i> it was taken from the pool
         * as part of the {@link javax.sql.DataSource#getConnection() DataSource.getConnection()} flow.
         * Its execution should take as short time as possible.
         *
         * <p>Worth noting that since version 19.0 the {@code takenNanos} parameter represents only the
         * time waited for an object to become available in the pool, excluding any object creation time.
         *
         * @param rawConnection the retrieved from the pool <b>raw</b> JDBC Connection; <b>note that it can be
         *                      {@code null}</b> if we were unable to obtain a connection from the pool within the
         *                      specified time limit or if we attempted to create a new Connection in the pool and
         *                      the attempt failed with an exception
         * @param takenNanos the time taken to get this connection in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface CloseConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection <i>before</i> it is restored back to the
         * pool as part of the {@link Connection#close()} flow. Its execution should take as short time as possible.
         *
         * @param rawConnection the <b>raw</b> JDBC Connection that will be returned to the pool
         * @param takenNanos the time for which this connection was held by the application before it was restored
         *                   to the pool in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface DestroyConnection extends Hook {
        /**
         * A programming hook that will be invoked only once <i>after</i> the raw JDBC Connection is closed/destroyed.
         * Its execution should take as short time as possible.
         *
         * @param rawConnection the <b>raw</b> JDBC Connection that was just closed
         * @param takenNanos the time taken to close/destroy this connection in nanoseconds
         */
        void on(Connection rawConnection, long takenNanos);
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Invocation hooks:

    interface MethodInvocation extends Hook {
        /**
         * An application hook that will be invoked <i>before</i> a method on any of the proxied JDBC interfaces is
         * invoked. Its execution should take as short time as possible.
         *
         * <p><b>Note that</b> any method invocations on the hook {@code Object proxy} parameter will recursively
         * re-enter this same hook and must be done with extreme care as they may cause infinite recursion and
         * a {@code StackOverflowError}.
         *
         * <p>For further implementation details see the comments for
         * {@link org.vibur.dbcp.proxy.InvocationHooksHolder#onMethodInvocation onMethodInvocation}.
         *
         * @param proxy the JDBC object <b>proxy</b> that the method was invoked on;
         *              this can be the Connection proxy, the Statement proxy, etc
         * @param method the invoked method
         * @param args the method arguments
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Object proxy, Method method, Object[] args) throws SQLException;
    }

    interface StatementExecution extends Hook {
        /**
         * An application hook that will be invoked <i>around</i> the call of each JDBC Statement "execute..." method.
         * Its execution should take as short time as possible. An example implementation may look like:
         * <pre>{@code
         *
         *      public Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
         *                       StatementProceedingPoint proceed) throws SQLException {
         *          try {
         *              // do something before the real method execution, for example, increment a queriesInProcess counter
         *              // or start a stopwatch
         *
         *              Object result = proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed); // execute it
         *
         *              // examine a thrown SQLException if necessary, retry the Statement execution if appropriate, etc
         *
         *              return result;
         *
         *          } finally {
         *              // do something after the real method execution, for example, decrement a queriesInProcess counter
         *              // or stop a stopwatch
         *          }
         *      }
         * }</pre>
         *
         * @param proxy the Statement <b>proxy</b> instance that the method was invoked on
         * @param method the invoked method
         * @param args the method arguments
         * @param sqlQuery the executed SQL query or prepared/callable SQL statement
         * @param sqlQueryParams the executed SQL query params if {@link ViburConfig#includeQueryParameters} is enabled
         *                       and if the {@code sqlQuery} was a prepared/callable SQL statement; {@code null} otherwise.
         *
         *                       <p>The size of the parameters list is equal to the number of the question mark placeholders
         *                       in the PreparedStatement query. Each Object[] inside the list contains at index 0
         *                       the name of the invoked setXyz method and at the following indices the parameters of
         *                       the invoked setXyz method. For an example, see the documentation for
         *                       {@link ResultSetRetrieval#on ResultSetRetrieval}.
         *
         * @param proceed the proceeding point through which the hook can pass the call to the intercepted Statement
         *                "execute..." method or to the next registered {@code StatementExecution} around hook,
         *                if there is such
         * @return the result from the invocation of the original intercepted Statement "execute..." method
         * @throws SQLException if the underlying method throws such or to indicate an error
         */
        Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
                  StatementProceedingPoint proceed) throws SQLException;
    }

    interface StatementProceedingPoint extends StatementExecution { }

    interface ResultSetRetrieval extends Hook {
        /**
         * An application hook that will be invoked <i>at the end</i> of each ResultSet retrieval as part of the
         * {@link java.sql.ResultSet#close() ResultSet.close()} flow. For implementation details, see the comments for
         * {@link ViburConfig#logLargeResultSet}. Its execution should take as short time as possible.
         *
         * @param sqlQuery the executed SQL query or prepared/callable SQL statement
         * @param sqlQueryParams the executed SQL query params if {@link ViburConfig#includeQueryParameters} is enabled
         *                       and if the {@code sqlQuery} was a prepared/callable SQL statement; {@code null} otherwise.
         *
         *                       <p>The size of the parameters list is equal to the number of the question mark placeholders
         *                       in the PreparedStatement query. Each Object[] inside the list contains at index 0
         *                       the name of the invoked setXyz method and at the following indices the parameters of
         *                       the invoked setXyz method; the last implies that the type of the parameter at index 0
         *                       is always a String, and at index 1 is always an Integer (as the first parameter of each
         *                       setXyz method is always an int). For example, for the PreparedStatement query
         *                       {@code select * from T1 where X=? and Y=? and Z>?} where X is of type String,
         *                       Y is of type Integer, Z is of type java.sql.Date, and the supplied parameters values
         *                       are "street", "22", and "2007/10/15", the resultant {@code sqlQueryParams} list will
         *                       look like:
         *                       <pre>{@code
         *                          List {
         *                              Object[] {String("setString"), Integer("1"), String("street")},
         *                              Object[] {String("setInt"), Integer("2"), Integer("22")},
         *                              Object[] {String("setDate"), Integer("3"), java.sql.Date("2007", "10", "15")}
         *                          }
         *                       }</pre>
         * @param resultSetSize the retrieved ResultSet size
         */
        void on(String sqlQuery, List<Object[]> sqlQueryParams, long resultSetSize);
    }
}
