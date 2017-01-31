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
import org.vibur.dbcp.ViburDBCPException;
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.cache.StatementHolder;
import org.vibur.dbcp.cache.StatementMethod;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.PoolOperations;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.vibur.dbcp.proxy.Proxy.*;

/**
 * @author Simeon Malchev
 */
public class ConnectionInvocationHandler extends AbstractInvocationHandler<Connection> {

    private final ConnHolder conn;
    private final PoolOperations poolOperations;
    private final ViburConfig config;

    private final StatementCache statementCache;

    ConnectionInvocationHandler(ConnHolder conn, PoolOperations poolOperations, ViburConfig config) {
        super(conn.value(), config, null /* becomes a new ExceptionCollector */);
        this.conn = conn;
        this.poolOperations = poolOperations;
        this.config = config;
        this.statementCache = config.getStatementCache();
    }

    @Override
    Object unrestrictedInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose();
        if (methodName == "isClosed")
            return isClosed();
        if (methodName == "isValid")
            return isClosed() ? false : targetInvoke(method, args);
        if (methodName == "abort")
            return processAbort(method, args);

        return super.unrestrictedInvoke(proxy, method, args);
    }

    @Override
    Object restrictedInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        // Methods which results have to be proxied so that when getConnection() is called
        // on their results the return value to be the current JDBC Connection proxy.
        if (methodName == "createStatement") { // *3
            StatementHolder statement = getUncachedStatement(method, args, null);
            return newProxyStatement(statement, proxy, config, this);
        }
        if (methodName == "prepareStatement") { // *6
            StatementHolder pStatement = getCachedStatement(method, args);
            return newProxyPreparedStatement(pStatement, proxy, config, this);
        }
        if (methodName == "prepareCall") { // *3
            StatementHolder cStatement = getCachedStatement(method, args);
            return newProxyCallableStatement(cStatement, proxy, config, this);
        }
        if (methodName == "getMetaData") { // *1
            DatabaseMetaData rawDatabaseMetaData = (DatabaseMetaData) targetInvoke(method, args);
            return newProxyDatabaseMetaData(rawDatabaseMetaData, proxy, config, this);
        }

        return super.restrictedInvoke(proxy, method, args);
    }

    /**
     * Returns <i>a possibly</i> cached StatementHolder object for the given proxied Connection object and the
     * invoked on it "prepare..." Method with the given args.
     *
     * @param method the invoked method
     * @param args the invoked method arguments
     * @return a retrieved from the cache or newly created StatementHolder object wrapping the raw JDBC Statement object
     * @throws Throwable if the invoked underlying "prepare..." method throws an exception
     */
    private StatementHolder getCachedStatement(Method method, Object[] args) throws Throwable {
        if (statementCache != null)
            return statementCache.take(new StatementMethod(this, method, args));

        return getUncachedStatement(method, args, (String) args[0]);
    }

    private StatementHolder getUncachedStatement(Method method, Object[] args, String sqlQuery) throws Throwable {
        Statement rawStatement = (Statement) targetInvoke(method, args);
        return new StatementHolder(rawStatement, null, sqlQuery);
    }

    private Object processClose() {
        if (close())
            poolOperations.restore(conn, true, getExceptions());
        return null;
    }

    private Object processAbort(Method method, Object[] args) throws Throwable {
        if (!close())
            return null;
        try {
            return targetInvoke(method, args);
        } finally {
            poolOperations.restore(conn, false, getExceptions());
        }
    }

    public PreparedStatement newStatement(Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        if (methodName != "prepareStatement" && methodName != "prepareCall")
            throw new ViburDBCPException("Unexpected method passed to newStatement() " + method);
        return (PreparedStatement) targetInvoke(method, args);
    }

    public void invalidate() {
        if (close())
            poolOperations.restore(conn, false, getExceptions());
    }
}
