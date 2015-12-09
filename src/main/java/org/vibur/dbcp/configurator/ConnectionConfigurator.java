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

package org.vibur.dbcp.configurator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Programming configurator for JDBC Connections.
 *
 * @author Simeon Malchev
 */
public interface ConnectionConfigurator {

    /**
     * This method will be invoked on the retrieved from the pool raw JDBC Connection before it is proxied and given
     * to the application. The invocation of this method happens as part of the {@link DataSource#getConnection()}
     * flow, and should take as short time as possible.
     *
     * @param rawConnection the retrieved from the pool raw JDBC connection
     * @throws SQLException if any operation executed on the raw JDBC Connection throws an SQLException
     */
    void configure(Connection rawConnection) throws SQLException;
}
