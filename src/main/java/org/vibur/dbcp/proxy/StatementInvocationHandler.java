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
import org.vibur.dbcp.cache.ConnMethodKey;
import org.vibur.dbcp.cache.StatementVal;
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.vibur.dbcp.cache.StatementVal.AVAILABLE;
import static org.vibur.dbcp.cache.StatementVal.IN_USE;
import static org.vibur.dbcp.util.FormattingUtils.getQueryPrefix;
import static org.vibur.dbcp.util.FormattingUtils.toSQLString;
import static org.vibur.dbcp.util.SqlUtils.clearWarnings;
import static org.vibur.dbcp.util.SqlUtils.closeStatement;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class StatementInvocationHandler extends ChildObjectInvocationHandler<Connection, Statement> {

    private static final Logger logger = LoggerFactory.getLogger(StatementInvocationHandler.class);

    private final StatementVal statement;
    private final ConcurrentMap<ConnMethodKey, StatementVal> statementCache;
    private final ViburDBCPConfig config;

    private final boolean logSlowQuery;
    private final List<Object[]> queryParams;

    public StatementInvocationHandler(StatementVal statement,
                                      ConcurrentMap<ConnMethodKey, StatementVal> statementCache,
                                      Connection connectionProxy, ViburDBCPConfig config,
                                      ExceptionListener exceptionListener) {
        super(statement.value(), connectionProxy, "getConnection", exceptionListener);
        if (config == null)
            throw new NullPointerException();
        this.statement = statement;
        this.statementCache = statementCache;
        this.config = config;
        this.logSlowQuery = config.getLogQueryExecutionLongerThanMs() >= 0;
        this.queryParams = logSlowQuery ? new LinkedList<Object[]>() : null;
    }

    protected Object doInvoke(Statement proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose();
        if (methodName == "isClosed")
            return isClosed();

        ensureNotClosed(); // all other Statement interface methods cannot work if the JDBC Statement is closed

        if (methodName.startsWith("set")) // this intercepts all "set..." JDBC Prepared/Callable Statement methods
            return processSet(method, args);
        if (methodName.startsWith("execute")) // this intercepts all "execute..." JDBC Statement methods
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

        Statement rawStatement = statement.value();
        if (statement.state() != null) { // if this statement is in the cache
            if (config.isClearSQLWarnings())
                clearWarnings(rawStatement);
            if (!statement.state().compareAndSet(IN_USE, AVAILABLE)) // just mark it as available if its state was in_use
                closeStatement(rawStatement); // and close it if it was already evicted (while its state was in_use)
        } else
            closeStatement(rawStatement);
        return null;
    }

    private Object processCancel(Method method, Object[] args) throws Throwable {
        if (statementCache != null) {
            Statement target = getTarget();
            for (Map.Entry<ConnMethodKey, StatementVal> entry : statementCache.entrySet()) {
                StatementVal value = entry.getValue();
                if (value.value() == target) { // comparing with == as these JDBC Statements are cached objects
                    statementCache.remove(entry.getKey(), value);
                    break;
                }
            }
        }
        return targetInvoke(method, args);
    }

    private Object processSet(Method method, Object[] args) throws Throwable {
        if (logSlowQuery && args != null && args.length >= 2) {
            Object[] params = new Object[args.length + 1];
            params[0] = method.getName();
            System.arraycopy(args, 0, params, 1, args.length);
            queryParams.add(params);
        }
        return targetInvoke(method, args); // the real "set..." call
    }

    /**
     * Mainly exists to provide Statement.execute... methods timing logging.
     */
    private Object processExecute(Statement proxy, Method method, Object[] args) throws Throwable {
        long startTime = logSlowQuery ? System.currentTimeMillis() : 0L;

        try {
            // executeQuery result has to be proxied so that when getStatement() is called
            // on its result the return value to be the current JDBC Statement proxy.
            if (method.getName() == "executeQuery") // *1
                return newProxiedResultSet(proxy, method, args);
            else
                return targetInvoke(method, args); // the real "execute..." call
        } finally {
            if (logSlowQuery)
                logQuery(proxy, args, startTime);
        }
    }

    private ResultSet newProxiedResultSet(Statement proxy, Method method, Object[] args) throws Throwable {
        ResultSet rawResultSet = (ResultSet) targetInvoke(method, args);
        return Proxy.newResultSet(rawResultSet, proxy, args, queryParams, config, getExceptionListener());
    }


    private void logQuery(Statement proxy, Object[] args, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= config.getLogQueryExecutionLongerThanMs()) {
            StringBuilder message = new StringBuilder(4096).append(String.format("%s took %d ms:\n%s",
                    getQueryPrefix(config), timeTaken, toSQLString(proxy, args, queryParams)));
            if (config.isLogStackTraceForLongQueryExecution())
                message.append("\n").append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(message.toString());
        }
    }

    protected void logTargetInvoke(Method method, Object[] args, InvocationTargetException e) {
        if (method.getName().startsWith("execute")) {
            String message = String.format("%s:\n%s\n-- threw:",
                    getQueryPrefix(config), toSQLString(getTarget(), args, queryParams));
            logger.warn(message, e);
        } else
            super.logTargetInvoke(method, args, e);
    }
}
