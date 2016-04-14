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

package org.vibur.dbcp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.util.pool.ConnHolder;
import org.vibur.objectpool.PoolService;

import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;

/**
 * @author Simeon Malchev
 */
public final class ViburUtils {

    private static final Logger logger = LoggerFactory.getLogger(ViburUtils.class);

    private ViburUtils() {}

    public static String getPoolName(ViburDBCPConfig config) {
        PoolService<ConnHolder> pool = config.getPool();
        return format("%s (%d/%d)", config.getName(), pool.taken(), pool.remainingCreated());
    }

    public static String getStackTraceAsString(StackTraceElement[] stackTrace) {
        int i;
        for (i = 0; i < stackTrace.length; i++) {
            if (!stackTrace[i].getClassName().startsWith("org.vibur.dbcp")
                || stackTrace[i].getClassName().endsWith("Test"))
                break;
        }

        StringBuilder builder = new StringBuilder(4096);
        for ( ; i < stackTrace.length; i++)
            builder.append("  at ").append(stackTrace[i]).append('\n');
        return builder.toString();
    }

    public static Connection unrollSQLException(ViburDBCPException e) throws SQLException {
        Throwable cause = e.getCause();
        if (cause instanceof SQLException)
            throw (SQLException) cause;
        logger.error("Unexpected exception cause", e);
        throw e; // should not normally happen
    }
}
