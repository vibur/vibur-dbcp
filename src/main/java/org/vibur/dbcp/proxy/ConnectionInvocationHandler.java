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
import org.vibur.dbcp.cache.ConnMethodKey;
import org.vibur.dbcp.cache.StatementCache;
import org.vibur.dbcp.cache.StatementVal;
import org.vibur.dbcp.pool.ConnHolder;
import org.vibur.dbcp.pool.PoolOperations;
import org.vibur.dbcp.util.collector.ExceptionCollectorImpl;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;

/**
 * @author Simeon Malchev
 */
public class ConnectionInvocationHandler extends AbstractInvocationHandler<Connection> implements TargetInvoker {

    private final ConnHolder conn;
    private final ViburDBCPConfig config;

    private final PoolOperations poolOperations;

    public ConnectionInvocationHandler(ConnHolder conn, ViburDBCPConfig config) {
        super(conn.value(), config, new ExceptionCollectorImpl(config));
        this.conn = conn;
        this.config = config;
        this.poolOperations = config.getPoolOperations();
    }

    @Override
    protected Object doInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        boolean aborted = methodName == "abort";
        if (aborted || methodName == "close")
            return processCloseOrAbort(aborted, method, args);
        if (methodName == "isClosed")
            return isClosed();

        if (methodName == "isValid")
            return isClosed() ? false : targetInvoke(method, args);

        ensureNotClosed(); // all other Connection interface methods cannot work if the JDBC Connection is closed

        // Methods which results have to be proxied so that when getConnection() is called
        // on their results the return value to be the current JDBC Connection proxy.
        if (methodName == "createStatement") { // *3
            StatementVal statement = getUncachedStatement(method, args);
            return Proxy.newStatement(statement, proxy, config, getExceptionCollector());
        }
        if (methodName == "prepareStatement") { // *6
            StatementVal pStatement = getCachedStatement(method, args);
            return Proxy.newPreparedStatement(pStatement, proxy, config, getExceptionCollector());
        }
        if (methodName == "prepareCall") { // *3
            StatementVal cStatement = getCachedStatement(method, args);
            return Proxy.newCallableStatement(cStatement, proxy, config, getExceptionCollector());
        }
        if (methodName == "getMetaData") { // *1
            DatabaseMetaData rawDatabaseMetaData = (DatabaseMetaData) targetInvoke(method, args);
            return Proxy.newDatabaseMetaData(rawDatabaseMetaData, proxy, config, getExceptionCollector());
        }

        return super.doInvoke(proxy, method, args);
    }

    /**
     * Returns <i>a possibly</i> cached StatementVal object for the given proxied Connection object and the
     * invoked on it prepareXYZ Method with the given args.
     *
     * @param method the invoked method
     * @param args the invoked method arguments
     * @return a retrieved from the cache or newly created StatementVal holder object wrapping the raw JDBC Statement object
     * @throws Throwable if the invoked underlying prepareXYZ method throws an exception
     */
    private StatementVal getCachedStatement(Method method, Object[] args) throws Throwable {
        StatementCache statementCache = config.getStatementCache();
        if (statementCache != null)
            return statementCache.retrieve(new ConnMethodKey(getTarget(), method, args), this);
        else
            return getUncachedStatement(method, args);
    }

    private StatementVal getUncachedStatement(Method method, Object[] args) throws Throwable {
        Statement rawStatement = (Statement) targetInvoke(method, args);
        return new StatementVal(rawStatement, null);
    }

    private Object processCloseOrAbort(boolean aborted, Method method, Object[] args) throws Throwable {
        if (aborted)
            targetInvoke(method, args); // executes the abort() call, which in turn may throw an exception
        if (!getAndSetClosed())
            poolOperations.restore(conn, aborted, getExceptionCollector().getExceptions());
        return null;
    }
}
