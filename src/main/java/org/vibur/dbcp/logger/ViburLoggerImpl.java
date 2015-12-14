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

package org.vibur.dbcp.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

import static org.vibur.dbcp.util.FormattingUtils.formatSql;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class ViburLoggerImpl implements ViburLogger {

    private static final Logger logger = LoggerFactory.getLogger(ViburLoggerImpl.class);

    public void logGetConnection(String poolName, Connection connProxy, long timeout, long timeTaken,
                                 StackTraceElement[] stackTrace) {
        StringBuilder log = new StringBuilder(4096)
                .append(String.format("Call to getConnection(%d) from pool %s took %d ms, connProxy = %s",
                        timeout, poolName, timeTaken, connProxy));
        if (stackTrace != null)
            log.append('\n').append(getStackTraceAsString(stackTrace));
        logger.warn(log.toString());
    }

    public void logQuery(String poolName, String sqlQuery, List<Object[]> queryParams, long timeTaken,
                         StackTraceElement[] stackTrace) {
        StringBuilder message = new StringBuilder(4096).append(String.format("SQL query execution from pool %s took %d ms:\n%s",
                poolName, timeTaken, formatSql(sqlQuery, queryParams)));
        if (stackTrace != null)
            message.append("\n").append(getStackTraceAsString(stackTrace));
        logger.warn(message.toString());
    }

    public void logResultSetSize(String poolName, String sqlQuery, List<Object[]> queryParams, long resultSetSize,
                                 StackTraceElement[] stackTrace) {
        StringBuilder message = new StringBuilder(4096).append(
                String.format("SQL query execution from pool %s retrieved a ResultSet with size %d:\n%s",
                        poolName, resultSetSize, formatSql(sqlQuery, queryParams)));
        if (stackTrace != null)
            message.append("\n").append(getStackTraceAsString(stackTrace));
        logger.warn(message.toString());
    }
}
