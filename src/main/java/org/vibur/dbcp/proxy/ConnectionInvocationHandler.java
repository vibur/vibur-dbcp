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
import org.vibur.dbcp.cache.MethodDefinition;
import org.vibur.dbcp.cache.MethodResult;
import org.vibur.dbcp.pool.ConnState;
import org.vibur.dbcp.pool.PoolOperations;
import org.vibur.dbcp.proxy.listener.ExceptionListenerImpl;
import org.vibur.objectpool.Holder;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Simeon Malchev
 */
public class ConnectionInvocationHandler extends AbstractInvocationHandler<Connection>
    implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionInvocationHandler.class);

    private final PoolOperations poolOperations;
    private final Holder<ConnState> hConnection;

    private final ViburDBCPConfig config;
    private final ConcurrentMap<MethodDefinition, MethodResult<Statement>> statementCache;

    public ConnectionInvocationHandler(Holder<ConnState> hConnection, ViburDBCPConfig config) {
        super(hConnection.value().connection(), new ExceptionListenerImpl());
        this.poolOperations = config.getPoolOperations();
        this.hConnection = hConnection;
        this.config = config;
        this.statementCache = config.getStatementCache();
    }

    @SuppressWarnings("unchecked")
    protected Object customInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "isValid")
            return targetInvoke(method, args);

        boolean aborted = methodName == "abort";
        if (aborted || methodName == "close")
            return processCloseOrAbort(aborted, method, args);
        if (methodName == "isClosed")
            return isClosed();

        ensureNotClosed(); // all other Connection interface methods cannot work if the JDBC Connection is closed

        // Methods which results have to be proxied so that when getConnection() is called
        // on their results the return value to be current JDBC Connection proxy.
        if (methodName == "createStatement") { // *3
            MethodResult<Statement> statementResult =
                (MethodResult<Statement>) getUncachedStatementResult(method, args);
            return Proxy.newStatement(statementResult, null, proxy, config, getExceptionListener());
        }
        if (methodName == "prepareStatement") { // *6
            MethodResult<PreparedStatement> statementResult =
                (MethodResult<PreparedStatement>) getStatementResult(method, args);
            return Proxy.newPreparedStatement(statementResult, statementCache, proxy, config,
                getExceptionListener());
        }
        if (methodName == "prepareCall") { // *3
            MethodResult<CallableStatement> statementResult =
                (MethodResult<CallableStatement>) getStatementResult(method, args);
            return Proxy.newCallableStatement(statementResult, statementCache, proxy, config,
                getExceptionListener());
        }
        if (methodName == "getMetaData") { // *1
            DatabaseMetaData metaData = (DatabaseMetaData) targetInvoke(method, args);
            return Proxy.newDatabaseMetaData(metaData, proxy, getExceptionListener());
        }

        return super.customInvoke(proxy, method, args);
    }

    private MethodResult<? extends Statement> getStatementResult(Method method, Object[] args) throws Throwable {
        if (statementCache != null) {
            Connection target = getTarget();
            MethodDefinition key = new MethodDefinition(target, method, args);
            MethodResult<Statement> statementResult = statementCache.get(key);
            if (statementResult == null || statementResult.inUse().getAndSet(true)) {
                Statement statement = (Statement) targetInvoke(method, args);
                if (statementResult == null) { // there was no entry for the key, so we'll try to put a new one
                    statementResult = new MethodResult<Statement>(statement, new AtomicBoolean(true));
                    if (statementCache.putIfAbsent(key, statementResult) != null)
                        // because another thread succeeded to put the entry before us
                        statementResult = new MethodResult<Statement>(statement, null);
                }
                return statementResult;
            } else { // the statementResult is valid and was not inUse
                if (logger.isTraceEnabled())
                    logger.trace("Using cached statement for connection {}, method {}, args {}",
                        target, method, Arrays.toString(args));
                return statementResult;
            }
        } else {
            return getUncachedStatementResult(method, args);
        }
    }

    private MethodResult<? extends Statement> getUncachedStatementResult(Method method, Object[] args) throws Throwable {
        Statement statement = (Statement) targetInvoke(method, args);
        return new MethodResult<Statement>(statement, null);
    }

    private Object processCloseOrAbort(boolean aborted, Method method, Object[] args) throws Throwable {
        if (getAndSetClosed())
            return null;
        try {
            return aborted ? targetInvoke(method, args) : null; // close() is not passed, abort() is passed
        } finally {
            poolOperations.restore(hConnection, aborted, getExceptionListener().getExceptions());
        }
    }
}
