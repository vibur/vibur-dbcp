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

package org.vibur.dbcp.util.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

import static java.lang.String.format;
import static org.vibur.dbcp.util.QueryUtils.formatSql;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * Vibur logging of long lasting getConnection() calls, slow SQL queries, and large ResultSets - operations implementation.
 *
 * <p>This class can be sub-classed by an application class that wants to intercept all such logging calls in order to
 * collect SQL queries execution statistics or similar, and that also wants to return the calls back to their
 * super-methods, as this will in turn allow the Vibur DBCP log to be created as normal.
 *
 * @author Simeon Malchev
 */
public class BaseViburLogger implements ViburLogger {

    private static final Logger logger = LoggerFactory.getLogger(BaseViburLogger.class);

    @Override
    public void logGetConnection(String poolName, Connection connProxy, long timeout, long timeTaken,
                                 StackTraceElement[] stackTrace) {
        StringBuilder log = new StringBuilder(4096)
                .append(format("Call to getConnection(%d) from pool %s took %d ms, connProxy = %s",
                        timeout, poolName, timeTaken, connProxy));
        if (stackTrace != null)
            log.append('\n').append(getStackTraceAsString(stackTrace));
        logger.warn(log.toString());
    }

    @Override
    public void logQuery(String poolName, String sqlQuery, List<Object[]> queryParams, long timeTaken,
                         StackTraceElement[] stackTrace) {
        StringBuilder message = new StringBuilder(4096).append(format("SQL query execution from pool %s took %d ms:\n%s",
                poolName, timeTaken, formatSql(sqlQuery, queryParams)));
        if (stackTrace != null)
            message.append("\n").append(getStackTraceAsString(stackTrace));
        logger.warn(message.toString());
    }

    @Override
    public void logResultSetSize(String poolName, String sqlQuery, List<Object[]> queryParams, long resultSetSize,
                                 StackTraceElement[] stackTrace) {
        StringBuilder message = new StringBuilder(4096).append(
                format("SQL query execution from pool %s retrieved a ResultSet with size %d:\n%s",
                        poolName, resultSetSize, formatSql(sqlQuery, queryParams)));
        if (stackTrace != null)
            message.append("\n").append(getStackTraceAsString(stackTrace));
        logger.warn(message.toString());
    }
}
