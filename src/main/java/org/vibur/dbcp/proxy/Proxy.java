/**
 * Copyright 2013-2025 Simeon Malchev
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

import java.lang.reflect.InvocationHandler;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static java.lang.reflect.Proxy.newProxyInstance;

/**
 * @author Simeon Malchev
 */
public final class Proxy {

    private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private static final Class<?>[] connectionClass = {Connection.class};
    private static final Class<?>[] statementClass = {Statement.class};
    private static final Class<?>[] pStatementClass = {PreparedStatement.class};
    private static final Class<?>[] cStatementClass = {CallableStatement.class};
    private static final Class<?>[] metadataClass = {DatabaseMetaData.class};
    private static final Class<?>[] resultSetClass = {ResultSet.class};

    private Proxy() { }

    public static Connection newProxyConnection(ConnHolder connHolder, PoolOperations poolOperations, ViburConfig config) {
        InvocationHandler handler = new ConnectionInvocationHandler(connHolder, poolOperations, config); // connHolder is never null
        return (Connection) newProxyInstance(classLoader, connectionClass, handler);
    }

    static Statement newProxyStatement(StatementHolder rawStatement, Connection connProxy,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        if (rawStatement == null) {
            return null;
        }

        InvocationHandler handler = new StatementInvocationHandler(
                rawStatement, null /* turns off the statement cache */, connProxy, config, exceptionCollector);
        return (Statement) newProxyInstance(classLoader, statementClass, handler);
    }

    static PreparedStatement newProxyPreparedStatement(StatementHolder rawPStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        if (rawPStatement == null) {
            return null;
        }

        InvocationHandler handler = new StatementInvocationHandler(
                rawPStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return (PreparedStatement) newProxyInstance(classLoader, pStatementClass, handler);
    }

    static CallableStatement newProxyCallableStatement(StatementHolder rawCStatement, Connection connProxy,
                                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        if (rawCStatement == null) {
            return null;
        }

        InvocationHandler handler = new StatementInvocationHandler(
                rawCStatement, config.getStatementCache(), connProxy, config, exceptionCollector);
        return (CallableStatement) newProxyInstance(classLoader, cStatementClass, handler);
    }

    static DatabaseMetaData newProxyDatabaseMetaData(DatabaseMetaData rawMetaData, Connection connProxy,
                                                     ViburConfig config, ExceptionCollector exceptionCollector) {
        if (rawMetaData == null) {
            return null;
        }

        InvocationHandler handler = new ChildObjectInvocationHandler<>(
                rawMetaData, connProxy, "getConnection", config, exceptionCollector);
        return (DatabaseMetaData) newProxyInstance(classLoader, metadataClass, handler);
    }

    static ResultSet newProxyResultSet(ResultSet rawResultSet, Statement statementProxy,
                                       String sqlQuery, List<Object[]> sqlQueryParams,
                                       ViburConfig config, ExceptionCollector exceptionCollector) {
        if (rawResultSet == null) {
            return null;
        }

        InvocationHandler handler = new ResultSetInvocationHandler(
                rawResultSet, statementProxy, sqlQuery, sqlQueryParams, config, exceptionCollector);
        return (ResultSet) newProxyInstance(classLoader, resultSetClass, handler);
    }
}
