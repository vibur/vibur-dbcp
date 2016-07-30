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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Defines the {@link ViburDBCPDataSource} lifecycle operations and states. Also, defines specific to Vibur
 * DataSource operations such as retrieving of a non-pooled connection and severing of a connection.
 *
 * @author Simeon Malchev
 */
public interface ViburDataSource extends DataSource, AutoCloseable {

    /**
     * The possible states in which the DataSource can be. The transition of the states is NEW-&gt;WORKING-&gt;TERMINATED.
     */
    enum State {
        NEW {
            @Override
            public String toString() {
                return "Vibur DBCP is not started.";
            }
        },
        WORKING {
            @Override
            public String toString() {
                return "Vibur DBCP is working.";
            }
        },
        TERMINATED {
            @Override
            public String toString() {
                return "Vibur DBCP is terminated.";
            }
        }
    }

    /**
     * Starts this DataSource. In order to be used, the implementing DataSource has to be
     * first created via calling one of the available constructors, configured, and then started
     * via calling this method. Any necessary validation of the configuration will be performed
     * as part of this call.
     *
     * @throws ViburDBCPException if not in a {@code NEW} state when started;
     *      if a configuration error is found during start;
     *      if cannot start this DataSource successfully for any other reason
     */
    void start();

    /**
     * Terminates this DataSource. Once terminated the DataSource cannot be more revived.
     */
    void terminate();

    /**
     * Returns this DataSource current state.
     */
    State getState();

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Returns a <b>raw (non-pooled)</b> JDBC Connection using the default username and password.
     *
     * @throws SQLException if an error occurs while creating the connection
     */
    Connection getNonPooledConnection() throws SQLException;

    /**
     * Returns a <b>raw (non-pooled)</b> JDBC Connection using the supplied username and password.
     *
     * @param username the database username
     * @param password the database password
     * @throws SQLException if an error occurs while creating the connection
     */
    Connection getNonPooledConnection(String username, String password) throws SQLException;

    /**
     * Severs the supplied connection which can either be a pooled connection retrieved via calling
     * one of the {@link #getConnection} methods or a raw non-pooled connection retrieved via calling
     * one of the {@link #getNonPooledConnection} methods. If the supplied connection is pooled, it will
     * be closed and removed from the pool and its underlying raw connection will be closed, too. In the
     * case when the supplied connection is non-pooled, it will be just closed.
     * <p>
     * Calling this method on a pooled or non-pooled connection that is already closed is a no-op.
     * When the {@code severConnection} method returns, the connection on which it is called will
     * be marked as closed.
     *
     * @param connection the connection to be severed
     * @throws SQLException if an error occurs while closing/severing the connection
     */
    void severConnection(Connection connection) throws SQLException;

    /**
     * A synonym for {@link #terminate()}. Overrides the {@link AutoCloseable}'s method in order to overrule
     * the throwing of a checked {@code Exception}.
     */
    @Override
    void close();
}
