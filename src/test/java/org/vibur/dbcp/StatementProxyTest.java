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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author Simeon Malchev
 */
public class StatementProxyTest extends AbstractDataSourceTest {

    @Test
    public void testSameStatement() throws SQLException {
        @SuppressWarnings("resource")
        var ds = createDataSourceNoStatementsCache();

        try (var connection = ds.getConnection();
             var statement = connection.createStatement()) {

            var resultSet = statement.executeQuery("select * from actor where first_name = 'CHRISTIAN'");
            assertSame(statement, resultSet.getStatement());
        }
    }
}
