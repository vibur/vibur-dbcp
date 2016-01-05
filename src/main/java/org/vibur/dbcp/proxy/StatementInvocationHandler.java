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
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.cache.StatementVal;
import org.vibur.dbcp.util.collector.ExceptionCollector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import static org.vibur.dbcp.util.QueryUtils.formatSql;
import static org.vibur.dbcp.util.QueryUtils.getSqlQuery;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;

/**
 * @author Simeon Malchev
 */
public class StatementInvocationHandler extends ChildObjectInvocationHandler<Connection, Statement>
        implements TargetInvoker {

    private static final Logger logger = LoggerFactory.getLogger(StatementInvocationHandler.class);

    private final StatementVal statementVal;
    private final StatementCache statementCache;
    private final ViburDBCPConfig config;

    private final boolean logSlowQuery;
    private final List<Object[]> queryParams;

    public StatementInvocationHandler(StatementVal statementVal, StatementCache statementCache,
                                      Connection connectionProxy, ViburDBCPConfig config,
                                      ExceptionCollector exceptionCollector) {
        super(statementVal.value(), connectionProxy, "getConnection", config, exceptionCollector);
        if (config == null)
            throw new NullPointerException();
        this.statementVal = statementVal;
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

        if (statementCache != null)
            statementCache.restore(statementVal, config.isClearSQLWarnings());
        return null;
    }

    private Object processCancel(Method method, Object[] args) throws Throwable {
        if (statementCache != null)
            statementCache.remove(getTarget(), false);

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
        return Proxy.newResultSet(rawResultSet, proxy, args, queryParams, config, getExceptionCollector());
    }

    private void logQuery(Statement proxy, Object[] args, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= config.getLogQueryExecutionLongerThanMs())
            config.getViburLogger().logQuery(
                    getPoolName(config), getSqlQuery(proxy, args), queryParams, timeTaken,
                    config.isLogStackTraceForLongQueryExecution() ? new Throwable().getStackTrace() : null);
    }

    protected void logInvokeFailure(Method method, Object[] args, InvocationTargetException e) {
        if (method.getName().startsWith("execute")) {
            logger.warn("SQL query execution from pool {}:\n{}\n-- threw:",
                    getPoolName(config), formatSql(getSqlQuery(getTarget(), args), queryParams), e);
        } else
            super.logInvokeFailure(method, args, e);
    }
}
