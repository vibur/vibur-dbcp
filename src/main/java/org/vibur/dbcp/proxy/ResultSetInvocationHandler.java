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

package org.vibur.dbcp.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.vibur.dbcp.util.SqlUtils.getQueryPrefix;
import static org.vibur.dbcp.util.SqlUtils.toSQLString;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class ResultSetInvocationHandler extends ChildObjectInvocationHandler<Statement, ResultSet> {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetInvocationHandler.class);

    private final Object[] executeMethodArgs;
    private final List<Object[]> queryParams;
    private final ViburDBCPConfig config;

    private final AtomicLong resultSetSize = new AtomicLong(0);

    public ResultSetInvocationHandler(ResultSet rawResultSet, Statement statementProxy,
                                      Object[] executeMethodArgs, List<Object[]> queryParams,
                                      ViburDBCPConfig config, ExceptionListener exceptionListener) {
        super(rawResultSet, statementProxy, "getStatement", exceptionListener);
        this.executeMethodArgs = executeMethodArgs;
        this.queryParams = queryParams;
        this.config = config;
    }

    protected Object doInvoke(ResultSet proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose(method, args);

        ensureNotClosed();

        if (methodName == "next")
            return processNext(method, args);

        return super.doInvoke(proxy, method, args);
    }

    private Object processNext(Method method, Object[] args) throws Throwable {
        resultSetSize.incrementAndGet();
        return targetInvoke(method, args);
    }

    private Object processClose(Method method, Object[] args) throws Throwable {
        logResultSetSize();
        return targetInvoke(method, args);
    }

    private void logResultSetSize() {
        long size = resultSetSize.get() - 1;
        if (config.getLogLargeResultSet() < 0 || size < 0 || config.getLogLargeResultSet() > size)
            return;

        StringBuilder message = new StringBuilder(4096).append(
                String.format("%s retrieved a ResultSet with size %d:\n%s", getQueryPrefix(config),
                        size, toSQLString(getParentProxy(), executeMethodArgs, queryParams)));
        if (config.isLogStackTraceForLargeResultSet())
            message.append("\n").append(getStackTraceAsString(new Throwable().getStackTrace()));
        logger.warn(message.toString());
    }
}
