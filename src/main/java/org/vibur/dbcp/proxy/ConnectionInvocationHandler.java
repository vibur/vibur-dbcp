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
import org.vibur.dbcp.cache.ConnMethod;
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.cache.StatementHolder;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.PoolOperations;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;

import static org.vibur.dbcp.proxy.Proxy.*;

/**
 * @author Simeon Malchev
 */
public class ConnectionInvocationHandler extends AbstractInvocationHandler<Connection> {

    private final ConnHolder conn;
    private final PoolOperations poolOperations;
    private final ViburConfig config;

    ConnectionInvocationHandler(ConnHolder conn, PoolOperations poolOperations, ViburConfig config) {
        super(conn.value(), config, new ExceptionCollector(config));
        this.conn = conn;
        this.poolOperations = poolOperations;
        this.config = config;
    }

    @Override
    Object doInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "close")
            return processClose();
        if (methodName == "isClosed")
            return isClosed();
        if (methodName == "isValid")
            return isClosed() ? false : targetInvoke(method, args);
        if (methodName == "abort")
            return processAbort(method, args);

        ensureNotClosed(); // all other Connection interface methods cannot work if the JDBC Connection is closed

        // Methods which results have to be proxied so that when getConnection() is called
        // on their results the return value to be the current JDBC Connection proxy.
        if (methodName == "createStatement") { // *3
            StatementHolder statement = getUncachedStatement(method, args);
            return newProxyStatement(statement, proxy, config, getExceptionCollector());
        }
        if (methodName == "prepareStatement") { // *6
            StatementHolder pStatement = getCachedStatement(method, args);
            return newProxyPreparedStatement(pStatement, proxy, config, getExceptionCollector());
        }
        if (methodName == "prepareCall") { // *3
            StatementHolder cStatement = getCachedStatement(method, args);
            return newProxyCallableStatement(cStatement, proxy, config, getExceptionCollector());
        }
        if (methodName == "getMetaData") { // *1
            DatabaseMetaData rawDatabaseMetaData = (DatabaseMetaData) targetInvoke(method, args);
            return newProxyDatabaseMetaData(rawDatabaseMetaData, proxy, config, getExceptionCollector());
        }

        return super.doInvoke(proxy, method, args);
    }

    /**
     * Returns <i>a possibly</i> cached StatementHolder object for the given proxied Connection object and the
     * invoked on it prepareXYZ Method with the given args.
     *
     * @param method the invoked method
     * @param args the invoked method arguments
     * @return a retrieved from the cache or newly created StatementHolder object wrapping the raw JDBC Statement object
     * @throws Throwable if the invoked underlying prepareXYZ method throws an exception
     */
    private StatementHolder getCachedStatement(Method method, Object[] args) throws Throwable {
        StatementCache statementCache = config.getStatementCache();
        if (statementCache != null)
            return statementCache.computeIfAbsent(new ConnMethod(getTarget(), method, args), this);

        return getUncachedStatement(method, args);
    }

    private StatementHolder getUncachedStatement(Method method, Object[] args) throws Throwable {
        Statement rawStatement = (Statement) targetInvoke(method, args);
        return new StatementHolder(rawStatement, null);
    }

    private Object processClose() {
        if (!getAndSetClosed())
            restoreToPool(true);
        return null;
    }

    private Object processAbort(Method method, Object[] args) throws Throwable {
        if (getAndSetClosed())
            return null;
        try {
            return targetInvoke(method, args);
        } finally {
            restoreToPool(false);
        }
    }

    private void restoreToPool(boolean valid) {
        poolOperations.restore(conn, valid, getExceptionCollector().getExceptions());
    }

    public void invalidate() {
        getAndSetClosed();
        restoreToPool(false);
    }
}
