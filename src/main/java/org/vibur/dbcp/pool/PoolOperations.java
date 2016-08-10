/**
 * Copyright 2014 Simeon Malchev
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Defines the facade operations through which the {@link ConnectionFactory}  and {@link org.vibur.objectpool.PoolService}
 * functions are accessed by {@link org.vibur.dbcp.ViburDBCPDataSource}. Essentially, these are operations that allows
 * us to get and restore a JDBC connection from the pool, as well as to process the SQLExceptions that might have
 * occurred on the taken JDBC Connection.
 *
 * @author Simeon Malchev
 */
public interface PoolOperations {

    Connection getProxyConnection(long timeout) throws SQLException;

    void restore(ConnHolder conn, boolean valid, List<Throwable> errors);
}
