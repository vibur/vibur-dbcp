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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Simeon Malchev
 */
public class StatementHookTest extends AbstractDataSourceTest {

    @Test
    public void testStatementExecutionHook() throws SQLException {
        @SuppressWarnings("resource")
        var ds = createDataSourceNotStarted();

        List<String> executionOrder = new ArrayList<>();

        // first hook
        ds.getInvocationHooks().addOnStatementExecution((proxy, method, args, sqlQuery, sqlQueryParams, proceed) -> {
            try {
                executionOrder.add("aa");
                return proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed);
            } finally {
                executionOrder.add("bb");
            }
        });

        // second hook
        ds.getInvocationHooks().addOnStatementExecution((proxy, method, args, sqlQuery, sqlQueryParams, proceed) -> {
            try {
                executionOrder.add("cc");
                return proceed.on(proxy, method, args, sqlQuery, sqlQueryParams, proceed);
            } finally {
                executionOrder.add("dd");
            }
        });

        ds.start();

        try (var connection = ds.getConnection();
             var statement = connection.createStatement()) {

            var resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            assertTrue(resultSet.next()); // make sure the PreparedStatement execution has returned at least one record
        }

        assertEquals("aa", executionOrder.get(0));
        assertEquals("cc", executionOrder.get(1));
        assertEquals("dd", executionOrder.get(2));
        assertEquals("bb", executionOrder.get(3));
    }
}
