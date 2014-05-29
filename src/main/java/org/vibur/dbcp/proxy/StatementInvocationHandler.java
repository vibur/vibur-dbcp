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
import org.vibur.dbcp.cache.MethodDef;
import org.vibur.dbcp.cache.ReturnVal;
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import static org.vibur.dbcp.cache.ReturnVal.AVAILABLE;
import static org.vibur.dbcp.cache.ReturnVal.EVICTED;
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

    private final ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache;

    public StatementInvocationHandler(ReturnVal<? extends Statement> statement,
                                      ConcurrentMap<MethodDef<Connection>, ReturnVal<Statement>> statementCache,
                                      Connection connectionProxy, ViburDBCPConfig config,
                                      ExceptionListener exceptionListener) {
        super(statement.value(), connectionProxy, "getConnection", exceptionListener);
        if (config == null)
            throw new NullPointerException();
        this.statement = statement;
        this.statementCache = statementCache;
        this.config = config;
    }

    protected Object doInvoke(Statement proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose(method, args);
        if (methodName == "isClosed")
            return isClosed();

        ensureNotClosed(); // all other Statement interface methods cannot work if the JDBC Statement is closed

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

    private Object processClose(Method method, Object[] args) throws Throwable {
        if (getAndSetClosed())
            return null;
        if (statementCache != null && statement.state() != null) { // if this statement is in the cache
            if (statement.state().getAndSet(AVAILABLE) == EVICTED) // just mark it as available
                closeStatement(statement.value()); // and close it if it was already evicted
            return null; // otherwise, don't pass the call to the underlying close method
        } else
            return targetInvoke(method, args);
    }

    private Object processCancel(Method method, Object[] args) throws Throwable {
        if (statementCache != null) {
            Statement target = getTarget();
            for (Iterator<ReturnVal<Statement>> i = statementCache.values().iterator(); i.hasNext(); ) {
                ReturnVal<Statement> returnVal = i.next();
                if (returnVal.value().equals(target)) {
                    i.remove();
                    break;
                }
            }
        }
        return targetInvoke(method, args);
    }

    /**
     * Mainly exists to provide Statement.execute... methods timing logging.
     */
    private Object processExecute(Statement proxy, Method method, Object[] args) throws Throwable {
        boolean shouldLog = config.getLogQueryExecutionLongerThanMs() >= 0;
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

    private void logQuery(Object[] args, long startTime) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (timeTaken >= config.getLogQueryExecutionLongerThanMs()) {
            StringBuilder log = new StringBuilder(String.format("SQL query -- %s -- execution took %d ms.",
                toSQLString(getTarget(), args), timeTaken));
            if (config.isLogStackTraceForLongQueryExecution())
                log.append(NEW_LINE).append(getStackTraceAsString(new Throwable().getStackTrace()));
            logger.warn(log.toString());
        }
    }

    private ResultSet newProxiedResultSet(Statement proxy, Method method, Object[] args) throws Throwable {
        ResultSet rawResultSet = (ResultSet) targetInvoke(method, args);
        return Proxy.newResultSet(rawResultSet, proxy, getExceptionListener());
    }
}
