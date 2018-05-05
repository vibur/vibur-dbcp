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

import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.pool.Hook;
import org.vibur.dbcp.pool.HookHolder.InvocationHooksAccessor;
import org.vibur.dbcp.stcache.StatementCache;
import org.vibur.dbcp.stcache.StatementHolder;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.vibur.dbcp.proxy.Proxy.newProxyResultSet;
import static org.vibur.dbcp.util.JdbcUtils.quietClose;

/**
 * @author Simeon Malchev
 */
class StatementInvocationHandler extends ChildObjectInvocationHandler<Connection, Statement>
        implements Hook.StatementProceedingPoint {

    private final StatementHolder statement;
    private final StatementCache statementCache; // always "null" (i.e. turned off) for simple JDBC Statements
    private final ViburConfig config;
    private final Deque<ResultSet> currentResultSets = new ArrayDeque<>();

    private final Hook.StatementExecution[] executionHooks;
    private final Hook.StatementExecution firstHook;
    private int hookIdx = 0;

    private final boolean logSqlQueryParams;
    private final List<Object[]> sqlQueryParams;

    StatementInvocationHandler(StatementHolder statement, StatementCache statementCache, Connection connProxy,
                               ViburConfig config, ExceptionCollector exceptionCollector) {
        super(statement.rawStatement(), connProxy, "getConnection", config, exceptionCollector);
        this.statement = statement;
        this.statementCache = statementCache;
        this.config = config;

        InvocationHooksAccessor invocationHooksAccessor = (InvocationHooksAccessor) config.getInvocationHooks();
        this.executionHooks = invocationHooksAccessor.onStatementExecution();
        this.firstHook = executionHooks.length > 0 ? executionHooks[0] : this;

        this.logSqlQueryParams = config.isIncludeQueryParameters() &&
                (executionHooks.length > 0 || invocationHooksAccessor.onResultSetRetrieval().length > 0);
        this.sqlQueryParams = logSqlQueryParams ? new ArrayList<Object[]>() : null;
    }

    @Override
    Object unrestrictedInvoke(Statement proxy, Method method, Object[] args) throws SQLException {
        String methodName = method.getName();

        if (methodName == "close") {
            return processClose(method, args);
        }
        if (methodName == "isClosed") {
            return isClosed();
        }

        return super.unrestrictedInvoke(proxy, method, args);
    }

    @Override
    Object restrictedInvoke(Statement proxy, Method method, Object[] args) throws SQLException {
        String methodName = method.getName();

        if (methodName.startsWith("set")) { // this intercepts all "set..." JDBC Prepared/Callable Statement methods
            return processSet(method, args);
        }
        if (methodName.startsWith("execute")) { // this intercepts all "execute..." JDBC Statement methods
            return processExecute(proxy, method, args);
        }

        if (methodName == "getMoreResults") { // *2
            return processMoreResults(method, args);
        }

        // Methods which results have to be proxied so that when getStatement() is called
        // on their results the return value to be the current JDBC Statement proxy.
        if (methodName == "getResultSet" || methodName == "getGeneratedKeys") { // *2
            return newProxiedResultSet(proxy, method, args, statement.getSqlQuery());
        }

        if (methodName == "cancel") {
            return processCancel(method, args);
        }

        return super.restrictedInvoke(proxy, method, args);
    }

    private Object processClose(Method method, Object[] args) throws SQLException {
        if (!close()) {
            return null;
        }

        closeAllResultSets();

        if (statementCache != null && statementCache.restore(statement, config.isClearSQLWarnings())) {
            return null; // calls to close() are not passed when the statement is restored successfully in the cache
        }
        return targetInvoke(method, args);
    }

    private Object processCancel(Method method, Object[] args) throws SQLException {
        if (statementCache != null) {
            statementCache.remove(statement); // because cancelled Statements are not longer valid
        }
        return targetInvoke(method, args);
    }

    private Object processSet(Method method, Object[] args) throws SQLException {
        if (logSqlQueryParams && args != null && args.length >= 2) {
            addSqlQueryParams(method, args);
        }
        return targetInvoke(method, args); // the real "set..." call
    }

    private Object processExecute(Statement proxy, Method method, Object[] args) throws SQLException {
        closeAllResultSets();

        if (statement.getSqlQuery() == null && args != null && args.length >= 1) { // a simple Statement "execute..." call
            statement.setSqlQuery((String) args[0]);
        }

        try {
            return firstHook.on(proxy, method, args, statement.getSqlQuery(), sqlQueryParams, this); // see the SPP implementation below
        } finally {
            prepareForNextExecution();
        }
    }

    private Object processMoreResults(Method method, Object[] args) throws SQLException {
        int current = Statement.CLOSE_ALL_RESULTS;
        if (args != null && args.length == 1) {
            current = (Integer) args[0];
        }

        if (current == Statement.CLOSE_CURRENT_RESULT) {
            quietClose(currentResultSets.pollLast());
        }
        else if (current == Statement.CLOSE_ALL_RESULTS) {
            closeAllResultSets();
        }

        return targetInvoke(method, args);
    }

    private void prepareForNextExecution() {
        if (sqlQueryParams != null) {
            sqlQueryParams.clear();
        }
        hookIdx = 0;
    }

    private ResultSet newProxiedResultSet(Statement proxy, Method method, Object[] args, String sqlQuery) throws SQLException {
        ResultSet rawResultSet = (ResultSet) targetInvoke(method, args);
        return addResultSet(newProxyResultSet(rawResultSet, proxy, sqlQuery, sqlQueryParams, config, this));
    }

    private void addSqlQueryParams(Method method, Object[] args) {
        Object[] params = new Object[args.length + 1];
        params[0] = method.getName().substring(3); // "set".length() == 3
        System.arraycopy(args, 0, params, 1, args.length);
        sqlQueryParams.add(params);
    }

    private ResultSet addResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            currentResultSets.addLast(resultSet);
        }
        return resultSet;
    }

    private void closeAllResultSets() {
        ResultSet next;
        while ((next = currentResultSets.pollFirst()) != null) {
            quietClose(next);
        }
    }

    //////// The StatementProceedingPoint implementation: ////////

    @Override
    public Object on(Statement proxy, Method method, Object[] args, String sqlQuery, List<Object[]> sqlQueryParams,
                     StatementProceedingPoint proceed) throws SQLException {

        if (++hookIdx < executionHooks.length) { // invoke the next statement execution hook, if any
            return executionHooks[hookIdx].on(proxy, method, args, sqlQuery, sqlQueryParams, this);
        }

        return doProcessExecute(proxy, method, args);
    }

    private Object doProcessExecute(Statement proxy, Method method, Object[] args) throws SQLException {
        // executeQuery result has to be proxied so that when getStatement() is called
        // on its result the return value to be the current JDBC Statement proxy.
        if (method.getName() == "executeQuery") { // *1
            return newProxiedResultSet(proxy, method, args, statement.getSqlQuery());
        }

        return targetInvoke(method, args); // the real "execute..." call
    }
}
