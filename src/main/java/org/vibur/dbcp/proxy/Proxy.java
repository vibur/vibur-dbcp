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
import org.vibur.dbcp.cache.StatementHolder;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.util.PoolOperations;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.sql.*;
import java.util.List;

import static java.lang.reflect.Proxy.getProxyClass;

/**
 * @author Simeon Malchev
 */
public final class Proxy {

    private Proxy() {}

    public static Connection newProxyConnection(ConnHolder conn, PoolOperations poolOperations, ViburConfig config) {
        InvocationHandler handler = new ConnectionInvocationHandler(conn, poolOperations, config);
        return (Connection) newProxy(connectionCtor, handler);
    }

    static Statement newProxyStatement(StatementHolder statement, Connection connProxy,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                statement, null, connProxy, config, exceptionCollector);
        return (Statement) newProxy(statementCtor, handler);
    }

    static PreparedStatement newProxyPreparedStatement(StatementHolder pStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                pStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return (PreparedStatement) newProxy(pStatementCtor, handler);
    }

    static CallableStatement newProxyCallableStatement(StatementHolder cStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                cStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return (CallableStatement) newProxy(cStatementCtor, handler);
    }

    static DatabaseMetaData newProxyDatabaseMetaData(DatabaseMetaData rawMetaData, Connection connProxy,
                                                     ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new ChildObjectInvocationHandler<>(
                rawMetaData, connProxy, "getConnection", config, exceptionCollector);
        return (DatabaseMetaData) newProxy(metadataCtor, handler);
    }

    static ResultSet newProxyResultSet(ResultSet rawResultSet, Statement statementProxy,
                                       Object[] executeMethodArgs, List<Object[]> queryParams,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new ResultSetInvocationHandler(
                rawResultSet, statementProxy, executeMethodArgs, queryParams, config, exceptionCollector);
        return (ResultSet) newProxy(resultSetCtor, handler);
    }

    private static Object newProxy(Constructor<?> proxyCtor, InvocationHandler invocationHandler) {
        try {
            return proxyCtor.newInstance(invocationHandler);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }

    private static final Constructor<?> connectionCtor;
    private static final Constructor<?> statementCtor;
    private static final Constructor<?> pStatementCtor;
    private static final Constructor<?> cStatementCtor;
    private static final Constructor<?> metadataCtor;
    private static final Constructor<?> resultSetCtor;

    private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    // static initializer for all constructors:
    static {
        try {
            connectionCtor = getProxyClass(classLoader, Connection.class).getConstructor(InvocationHandler.class);
            statementCtor  = getProxyClass(classLoader, Statement.class).getConstructor(InvocationHandler.class);
            pStatementCtor = getProxyClass(classLoader, PreparedStatement.class).getConstructor(InvocationHandler.class);
            cStatementCtor = getProxyClass(classLoader, CallableStatement.class).getConstructor(InvocationHandler.class);
            metadataCtor   = getProxyClass(classLoader, DatabaseMetaData.class).getConstructor(InvocationHandler.class);
            resultSetCtor  = getProxyClass(classLoader, ResultSet.class).getConstructor(InvocationHandler.class);
        } catch (NoSuchMethodException e) {
            throw new Error(e);
        }
    }
}
