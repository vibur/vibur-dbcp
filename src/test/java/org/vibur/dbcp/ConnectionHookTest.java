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

/**
 * @author Simeon Malchev
 */
public class ConnectionHookTest extends AbstractDataSourceTest {

    @Test
    public void testInitGetConnectionHooks() throws SQLException {
        final List<String> executionOrder = new ArrayList<>();

        var ds = createDataSourceNotStarted();
        ds.setPoolInitialSize(0);
        ds.setPoolMaxSize(1);
        ds.getConnHooks().addOnInit((rawConnection, takenNanos) -> executionOrder.add("init"));
        ds.getConnHooks().addOnGet((rawConnection, takenNanos) -> executionOrder.add("get"));
        ds.start();

        var connection = ds.getConnection();
        connection.close();

        assertEquals(2, executionOrder.size());
        assertEquals("init", executionOrder.get(0));
        assertEquals("get", executionOrder.get(1));

        connection = ds.getConnection();
        connection.close();

        assertEquals(3, executionOrder.size());
        assertEquals("get", executionOrder.get(2));
    }
}
