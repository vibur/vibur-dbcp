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
import org.vibur.dbcp.cache.ConnMethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.vibur.dbcp.cache.ReturnVal.AVAILABLE;
import static org.vibur.dbcp.cache.ReturnVal.IN_USE;
import static org.vibur.dbcp.util.SqlUtils.closeStatement;
import static org.vibur.dbcp.util.SqlUtils.toSQLString;
import static org.vibur.dbcp.util.ViburUtils.NEW_LINE;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class StatementInvocationHandler extends ChildObjectInvocationHandler<Connection, Statement> {

    private static final Logger logger = LoggerFactory.getLogger(StatementInvocationHandler.class);

    private final ReturnVal<? extends Statement> statement;
    private final ViburDBCPConfig config;

    private final boolean shouldLog;
    private final List<Object[]> executeParams;

    private final ConcurrentMap<ConnMethodDef, ReturnVal<Statement>> statementCache;

    public StatementInvocationHandler(ReturnVal<? extends Statement> statement,
                                      ConcurrentMap<ConnMethodDef, ReturnVal<Statement>> statementCache,
                                      Connection connectionProxy, ViburDBCPConfig config,
                                      ExceptionListener exceptionListener) {
        super(statement.value(), connectionProxy, "getConnection", exceptionListener);
        if (config == null)
            throw new NullPointerException();
        this.statement = statement;
        this.statementCache = statementCache;
        this.config = config;
        this.shouldLog = config.getLogQueryExecutionLongerThanMs() >= 0;
        this.executeParams = this.shouldLog ? new LinkedList<Object[]>() : null;
    }

    protected Object doInvoke(Statement proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose();
        if (methodName == "isClosed")
            return isClosed();

        ensureNotClosed(); // all other Statement interface methods cannot work if the JDBC Statement is closed

        if (methodName.startsWith("set")) // this intercepts all "set..." JDBC Statements methods
            return processSet(method, args);
        if (methodName.startsWith("execute")) // this intercepts all "execute..." JDBC Statements methods
            return processExecute(proxy, method, args);

        // Methods which results have to be proxied so that when getStatement() is called
        // on their results the return value to be the current JDBC Statement proxy.
        if (methodName == "getResultSet" || methodName == "getGeneratedKeys") // *2
            return newProxiedResultSet(proxy, method, args);

        if (methodName == "cancel")
            return processCancel(method, args);

        return super.doInvoke(proxy, method, args);
    }

    private Object processClose() {
        if (getAndSetClosed())
            return null;
        if (statementCache != null && statement.state() != null) { // if this statement is in the cache
            if (!statement.state().compareAndSet(IN_USE, AVAILABLE)) // just mark it as available if it was in_use
                closeStatement(statement.value()); // and close it if it was already evicted
        } else
            closeStatement(statement.value());
        return null;
    }

    private Object processCancel(Method method, Object[] args) throws Throwable {
        if (statementCache != null) {
            Statement target = getTarget();
            for (Map.Entry<ConnMethodDef, ReturnVal<Statement>> entry : statementCache.entrySet()) {
                ReturnVal<Statement> value = entry.getValue();
                if (value.value() == target) { // comparing with == as these JDBC Statements are cached objects
                    statementCache.remove(entry.getKey(), value);
                    break;
                }
            }
        }
        return targetInvoke(method, args);
    }

    private Object processSet(Method method, Object[] args) throws Throwable {
        if (shouldLog && args != null && args.length >= 2) {
            Object[] params = new Object[args.length + 1];
            params[0] = method.getName();
            System.arraycopy(args, 0, params, 1, args.length);
            executeParams.add(params);
        }
        return targetInvoke(method, args); // the real "set..." call
    }

    /**
     * Mainly exists to provide Statement.execute... methods timing logging.
     */
    private Object processExecute(Statement proxy, Method method, Object[] args) throws Throwable {
        long startTime = shouldLog ? System.currentTimeMillis() : 0L;

        try {
            // executeQuery result has to be proxied so that when getStatement() is called
            // on its result the return value to be the current JDBC Statement proxy.
            if (method.getName() == "executeQuery") // *1
                return newProxiedResultSet(proxy, method, args);
            else
                return targetInvoke(method, args); // the real "execute..." call
        } finally {
            if (shouldLog)
                logQuery(args, startTime);
        }
    }

    private ResultSet newProxiedResultSet(Statement proxy, Method method, Object[] args) throws Throwable {
        ResultSet rawResultSet = (ResultSet) targetInvoke(method, args);
        return Proxy.newResultSet(rawResultSet, proxy, getExceptionListener());
    }

    private void logQuery(Object[] args, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= config.getLogQueryExecutionLongerThanMs()) {
            StringBuilder log = new StringBuilder(String.format("SQL query execution took %d ms:\n%s",
                    timeTaken, toSQLString(getTarget(), args, executeParams)));
            if (config.isLogStackTraceForLongQueryExecution())
                log.append(NEW_LINE).append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(log.toString());
        }
    }
}
