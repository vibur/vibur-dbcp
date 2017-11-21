/**
 * Copyright 2017 Simeon Malchev
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
import org.vibur.dbcp.pool.Hook;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Simeon Malchev
 */
public class StatementHookTest extends AbstractDataSourceTest {

    @Test
    public void testStatementExecutionHook() throws SQLException {
        final List<String> executionOrder = new ArrayList<>();

        ViburDBCPDataSource ds = createDataSourceNotStarted();
        ds.getInvocationHooks().addOnStatementExecution(new Hook.StatementExecution() {
            @Override
            public Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
                             StatementProceedingPoint proceed) throws SQLException {
                try {
                    executionOrder.add("aa");
                    return proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed);
                } finally {
                    executionOrder.add("bb");
                }
            }
        });
        ds.getInvocationHooks().addOnStatementExecution(new Hook.StatementExecution() {
            @Override
            public Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
                             StatementProceedingPoint proceed) throws SQLException {
                try {
                    executionOrder.add("cc");
                    return proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed);
                } finally {
                    executionOrder.add("dd");
                }
            }
        });
        ds.start();

        try (Connection connection = ds.getConnection();
             Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            assertTrue(resultSet.next()); // make sure the PreparedStatement execution has returned at least one record
        }

        assertEquals("aa", executionOrder.get(0));
        assertEquals("cc", executionOrder.get(1));
        assertEquals("dd", executionOrder.get(2));
        assertEquals("bb", executionOrder.get(3));
    }
}
