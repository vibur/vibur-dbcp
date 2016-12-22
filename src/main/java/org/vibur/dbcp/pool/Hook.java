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
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Objects.requireNonNull;

/**
 * An interface that holds all application programming hook interfaces for JDBC Connection tune-up and method 
 * invocation interception.
 * 
 * <p>These application hooks serve as extension points to the inner workings of the connection pool and have
 * access to the <b>raw (original)</b> JDBC Connection object, not the proxied such. In order to avoid interference
 * with how the connection pool manages its underlying connections, the application <b>must not</b> keep or store
 * in one or another form a reference to the {@code rawConnection} object.
 * 
 * @author Simeon Malchev
 */
public interface Hook {

    ////////////////////
    // Connection hooks:

    interface InitConnection extends Hook {
        /**
         * A programming hook that will be invoked only once when the raw JDBC Connection is first created. 
         * Its execution should take as short time as possible.
         *
         * @param rawConnection the just created raw JDBC connection
         * @param takenNanos the time taken to establish this connection in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface GetConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection as part of the 
         * {@link javax.sql.DataSource#getConnection()} flow. Its execution should take as short time as possible.
         *
         * @param rawConnection the retrieved from the pool raw JDBC connection; <b>note that</b> this can be {@code null}
         *                      if we were unable to obtain a connection from the pool within the specified time limit
         * @param takenNanos the time taken to get this connection from the pool in nanoseconds
         * @throws SQLException to indicate that an SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface CloseConnection extends Hook {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection as part of the 
         * {@link Connection#close()} flow. Its execution should take as short time as possible.
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
         * A programming hook that will be invoked only once when the raw JDBC Connection is closed/destroyed. 
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
         * An application hook that will be invoked when a method on any of the proxied JDBC interfaces is invoked.
         * The execution of this hook should take as short time as possible.
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
         */
        void on(String sqlQuery, List<Object[]> queryParams, long takenNanos);
    }

    interface ResultSetRetrieval extends Hook {
        /**
         * An application hook that will be invoked at the end of each ResultSet retrieval. For implementation details
         * see the comments for {@link ViburConfig#logLargeResultSet}. Its execution should take as short time
         * as possible.
         *
         * @param sqlQuery the executed SQL query or prepared/callable SQL statement
         * @param queryParams the executed SQL query params if {@link ViburConfig#includeQueryParameters} is enabled,
         *                    {@code null} otherwise
         * @param resultSetSize the retrieved ResultSet size
         */
        void on(String sqlQuery, List<Object[]> queryParams, long resultSetSize);
    }

    ///////////////
    // Hooks utils:

    final class Util {

        private Util() {}

        public static <T extends Hook> List<T> addHook(List<T> hooks, T hook) {
            requireNonNull(hook);
            if (hooks == EMPTY_LIST)
                hooks = new ArrayList<>();
            hooks.add(hook);
            return hooks;
        }
    }
}
