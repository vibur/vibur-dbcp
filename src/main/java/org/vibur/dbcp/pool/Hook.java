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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

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

    interface InitConnection {
        /**
         * A programming hook that will be invoked only once when the raw JDBC Connection is first created. 
         * Its execution should take as short time as possible.
         *
         * @param rawConnection the just created raw JDBC connection
         * @param takenNanos the time taken to establish this connection in nanoseconds
         * @throws SQLException to indicate that a SQL error has occurred
         */
        void on(Connection rawConnection, long takenNanos) throws SQLException;
    }

    interface GetConnection {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection as part of the 
         * {@link javax.sql.DataSource#getConnection()} flow. Its execution should take as short time as possible.
         *
         * @param rawConnection the retrieved from the pool raw JDBC connection
         * @throws SQLException to indicate that a SQL error has occurred
         */
        void on(Connection rawConnection) throws SQLException;
    }

    interface CloseConnection {
        /**
         * A programming hook that will be invoked on the raw JDBC Connection as part of the 
         * {@link Connection#close()} flow. Its execution should take as short time as possible.
         *
         * @param rawConnection the raw JDBC connection that will be returned to the pool
         * @throws SQLException to indicate that a SQL error has occurred
         */
        void on(Connection rawConnection) throws SQLException;
    }

    interface DestroyConnection {
        /**
         * A programming hook that will be invoked only once when the raw JDBC Connection is closed/destroyed. 
         * Its execution should take as short time as possible.
         *
         * @param rawConnection the raw JDBC connection that was just closed
         * @param takenNanos the time taken to close/destroy this connection in nanoseconds
         */
        void on(Connection rawConnection, long takenNanos);
    }


    interface MethodInvocation {
        /**
         * An application hook that will be invoked when a method on any of the proxied JDBC interfaces is invoked.
         * The execution of this method should take as short time as possible.
         *
         * @param proxy the proxy instance that the method was invoked on
         * @param method the invoked method
         * @param args the method arguments
         * @throws SQLException to indicate that a SQL error has occurred
         */
        void on(Object proxy, Method method, Object[] args) throws SQLException;
    }
}
