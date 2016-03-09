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

import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.cache.StatementVal;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.util.collector.ExceptionCollector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.sql.*;
import java.util.List;

/**
 * @author Simeon Malchev
 */
public final class Proxy {

    private Proxy() {}

    public static Connection newConnection(ConnHolder conn, ViburDBCPConfig config) {
        InvocationHandler handler = new ConnectionInvocationHandler(conn, config);
        return (Connection) newProxy(connectionCtor, handler);
    }

    public static Statement newStatement(StatementVal statement, Connection connectionProxy,
                                         ViburDBCPConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                statement, null, connectionProxy, config, exceptionCollector);
        return (Statement) newProxy(statementCtor, handler);
    }

    public static PreparedStatement newPreparedStatement(StatementVal pStatement, Connection connectionProxy,
                                                         ViburDBCPConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
                pStatement, config.getStatementCache(), connectionProxy, config, exceptionCollector);
        return (PreparedStatement) newProxy(pStatementCtor, handler);
    }

    public static CallableStatement newCallableStatement(StatementVal cStatement, Connection connectionProxy,
                                                         ViburDBCPConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new StatementInvocationHandler(
            cStatement, config.getStatementCache(), connectionProxy, config, exceptionCollector);
        return (CallableStatement) newProxy(cStatementCtor, handler);
    }

    public static DatabaseMetaData newDatabaseMetaData(DatabaseMetaData rawMetaData, Connection connectionProxy,
                                                       ViburDBCPConfig config, ExceptionCollector exceptionCollector) {
        InvocationHandler handler = new ChildObjectInvocationHandler<>(
            rawMetaData, connectionProxy, "getConnection", config, exceptionCollector);
        return (DatabaseMetaData) newProxy(metadataCtor, handler);
    }

    public static ResultSet newResultSet(ResultSet rawResultSet, Statement statementProxy,
                                         Object[] executeMethodArgs, List<Object[]> queryParams,
                                         ViburDBCPConfig config, ExceptionCollector exceptionCollector) {
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

    private static ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    // static initializer for all constructors:
    static {
        try {
            connectionCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                Connection.class).getConstructor(InvocationHandler.class);
            statementCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                Statement.class).getConstructor(InvocationHandler.class);
            pStatementCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                PreparedStatement.class).getConstructor(InvocationHandler.class);
            cStatementCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                CallableStatement.class).getConstructor(InvocationHandler.class);
            metadataCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                DatabaseMetaData.class).getConstructor(InvocationHandler.class);
            resultSetCtor = java.lang.reflect.Proxy.getProxyClass(classLoader,
                ResultSet.class).getConstructor(InvocationHandler.class);
        } catch (ReflectiveOperationException e) {
            throw new Error(e);
        }
    }
}
