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
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.PoolOperations;
import org.vibur.dbcp.stcache.StatementHolder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.sql.*;
import java.util.List;

import static java.lang.reflect.Proxy.getProxyClass;

/**
 * @author Simeon Malchev
 */
public final class Proxy {

    private Proxy() { }

    public static Connection newProxyConnection(ConnHolder connHolder, PoolOperations poolOperations, ViburConfig config) {
        InvocationHandler handler = new ConnectionInvocationHandler(connHolder, poolOperations, config);
        return newProxy(connectionCtor, handler);
    }

    static Statement newProxyStatement(StatementHolder statement, Connection connProxy,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                statement, null /* turns off the cache */, connProxy, config, exceptionCollector);
        return newProxy(statementCtor, handler);
    }

    static PreparedStatement newProxyPreparedStatement(StatementHolder pStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                pStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return newProxy(pStatementCtor, handler);
    }

    static CallableStatement newProxyCallableStatement(StatementHolder cStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                cStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return newProxy(cStatementCtor, handler);
    }

    static DatabaseMetaData newProxyDatabaseMetaData(DatabaseMetaData rawMetaData, Connection connProxy,
                                                     ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new ChildObjectInvocationHandler<>(
                rawMetaData, connProxy, "getConnection", config, exceptionCollector);
        return newProxy(metadataCtor, handler);
    }

    static ResultSet newProxyResultSet(ResultSet rawResultSet, Statement statementProxy,
                                       String sqlQuery, List<Object[]> sqlQueryParams,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new ResultSetInvocationHandler(
                rawResultSet, statementProxy, sqlQuery, sqlQueryParams, config, exceptionCollector);
        return newProxy(resultSetCtor, handler);
    }

    private static <T> T newProxy(Constructor<T> proxyCtor, InvocationHandler handler) {
        try {
            return proxyCtor.newInstance(handler);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private static final Constructor<Connection> connectionCtor;
    private static final Constructor<Statement> statementCtor;
    private static final Constructor<PreparedStatement> pStatementCtor;
    private static final Constructor<CallableStatement> cStatementCtor;
    private static final Constructor<DatabaseMetaData> metadataCtor;
    private static final Constructor<ResultSet> resultSetCtor;

    private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    // static initializer for all constructors:
    static {
        connectionCtor = getIHConstructor(Connection.class);
        statementCtor = getIHConstructor(Statement.class);
        pStatementCtor = getIHConstructor(PreparedStatement.class);
        cStatementCtor = getIHConstructor(CallableStatement.class);
        metadataCtor = getIHConstructor(DatabaseMetaData.class);
        resultSetCtor = getIHConstructor(ResultSet.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> Constructor<T> getIHConstructor(Class<T> cl) {
        try {
            return (Constructor<T>) getProxyClass(classLoader, cl).getConstructor(InvocationHandler.class);
        } catch (NoSuchMethodException e) {
            throw new Error(e);
        }
    }
}
