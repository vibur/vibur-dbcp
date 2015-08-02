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

package org.vibur.dbcp.util;

import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.objectpool.PoolService;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class FormattingUtils {

    private FormattingUtils() {}

    public static String toSQLString(Statement statement, Object[] args, List<Object[]> queryParams) {
        StringBuilder result = new StringBuilder(4096).append("-- ").append(statement instanceof PreparedStatement ?
                statement.toString() : Arrays.toString(args)); // the latter is for simple JDBC Statements
        if (!queryParams.isEmpty())
            result.append("\n-- Parameters:\n-- ").append(Arrays.deepToString(queryParams.toArray()));
        return result.toString();
    }

    public static String getQueryPrefix(ViburDBCPConfig config) {
        return "SQL query execution from pool " + getPoolName(config);
    }

    public static String getPoolName(ViburDBCPConfig config) {
        PoolService<ConnHolder> pool = config.getPool();
        return String.format("%s (%d/%d)", config.getName(), pool.taken(), pool.remainingCreated());
    }
}
