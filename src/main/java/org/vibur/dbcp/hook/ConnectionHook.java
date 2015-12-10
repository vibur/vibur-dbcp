/**
 * Copyright 2015 Simeon Malchev
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

package org.vibur.dbcp.hook;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Programming hook for JDBC Connections.
 *
 * @author Simeon Malchev
 */
public interface ConnectionHook {

    /**
     * An application hook which will be called on a retrieved from the pool raw JDBC Connection. The invocation of
     * this method will happen upon invoking methods from the {@link DataSource} or {@link Connection} interfaces,
     * and should take as short time as possible.
     *
     * @param rawConnection a retrieved from the pool raw JDBC connection
     * @throws SQLException if any operation executed on the raw JDBC Connection throws an SQLException
     */
    void on(Connection rawConnection) throws SQLException;
}
