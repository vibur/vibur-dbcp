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

package org.vibur.dbcp.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.cache.StatementKey;
import org.vibur.dbcp.cache.ValueHolder;
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import static org.vibur.dbcp.util.StatementUtils.toSQLString;
import static org.vibur.dbcp.util.ViburUtils.NEW_LINE;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class StatementInvocationHandler extends ConnectionChildInvocationHandler<Statement> {

    private static final Logger logger = LoggerFactory.getLogger(StatementInvocationHandler.class);

    private final ValueHolder<? extends Statement> statementHolder;
    private final ViburDBCPConfig config;

    private volatile boolean logicallyClosed = false;

    private final ConcurrentMap<StatementKey, ValueHolder<Statement>> statementCache;

    public StatementInvocationHandler(ValueHolder<? extends Statement> statementHolder,
                                      Connection connectionProxy,
                                      ViburDBCPConfig config,
                                      ExceptionListener exceptionListener) {
        super(statementHolder.value(), connectionProxy, exceptionListener);
        if (config == null)
            throw new NullPointerException();
        this.statementHolder = statementHolder;
        this.config = config;
        this.statementCache = config.getStatementCache();
    }

    protected Object customInvoke(Statement proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName.equals("close")) {
            return processClose(method, args);
        }
        if (methodName.equals("isClosed"))
            return logicallyClosed;

        // All other Statement interface methods cannot work if the JDBC Statement is closed:
        if (logicallyClosed)
            throw new SQLException("Statement is closed.");

        if (methodName.equals("cancel")) {
            return processCancel(proxy, method, args);
        }

        if (methodName.startsWith("execute")) // this intercepts all "execute..." JDBC Statements methods
            return processExecute(proxy, method, args);

        return super.customInvoke(proxy, method, args);
    }

    private Object processClose(Method method, Object[] args) throws Throwable {
        logicallyClosed = true;
        if (statementCache != null && statementHolder.inUse() != null) { // this statementHolder is in the cache
            statementHolder.inUse().set(false); // we just mark it as available
            return null; // and we don't pass the call to the underlying close method
        } else
            return targetInvoke(method, args);
    }

    private Object processCancel(Statement statementProxy, Method method, Object[] args) throws Throwable {
        if (statementCache != null)
            for (Iterator<ValueHolder<Statement>> i = statementCache.values().iterator(); i.hasNext(); ) {
                ValueHolder<Statement> valueHolder = i.next();
                if (valueHolder.value().equals(statementProxy)) {
                    i.remove();
                    break;
                }
            }
        return targetInvoke(method, args);
    }

    private Object processExecute(Statement statementProxy, Method method, Object[] args) throws Throwable {
        boolean shouldLog = config.getLogQueryExecutionLongerThanMs() >= 0;
        long startTime = shouldLog ? System.currentTimeMillis() : 0L;

        try {
            return targetInvoke(method, args); // the real "execute..." call
        } finally {
            if (shouldLog)
                logQuery(statementProxy, args, startTime);
        }
    }

    private void logQuery(Statement statementProxy, Object[] args, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= config.getLogQueryExecutionLongerThanMs()) {
            StringBuilder log = new StringBuilder(String.format("SQL query \"%s\" execution took %dms",
                toSQLString(statementProxy, args), timeTaken));
            if (config.isLogStackTraceForLongQueryExecution()) {
                log.append(NEW_LINE).append(getStackTraceAsString(new Throwable().getStackTrace()));
            }
            logger.warn(log.toString());
        }
    }
}
