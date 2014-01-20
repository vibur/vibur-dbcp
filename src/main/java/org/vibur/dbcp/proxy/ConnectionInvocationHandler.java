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
import org.vibur.dbcp.pool.ConnState;
import org.vibur.dbcp.ViburDBCPConfig;
import org.vibur.dbcp.cache.StatementKey;
import org.vibur.dbcp.cache.ValueHolder;
import org.vibur.dbcp.proxy.listener.ExceptionListenerImpl;
import org.vibur.objectpool.Holder;
import org.vibur.objectpool.HolderValidatingPoolService;

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

    private final HolderValidatingPoolService<ConnState> connectionPool;
    private final Holder<ConnState> hConnection;

    private final ViburDBCPConfig config;
    private final ConcurrentMap<StatementKey, ValueHolder<Statement>> statementCache;

    public ConnectionInvocationHandler(Holder<ConnState> hConnection, ViburDBCPConfig config) {
        super(hConnection.value().connection(), new ExceptionListenerImpl());
        this.connectionPool = config.getConnectionPool();
        if (this.connectionPool == null)
            throw new NullPointerException();
        this.hConnection = hConnection;
        this.config = config;
        this.statementCache = config.getStatementCache();
    }

    @SuppressWarnings("unchecked")
    protected Object customInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "isValid")
            return targetInvoke(method, args);

        boolean isCloseMethod = methodName == "close";
        if (isCloseMethod || methodName == "abort")
            return processCloseOrAbort(isCloseMethod, method, args);
        if (methodName == "isClosed")
            return isClosed();

        ensureNotClosed(); // all other Connection interface methods cannot work if the JDBC Connection is closed

        // Methods which results have to be proxied so that when getConnection() is called
        // on their results the return value to be current JDBC Connection proxy.
        if (methodName == "createStatement") { // *3
            ValueHolder<Statement> statementHolder =
                (ValueHolder<Statement>) getUncachedStatementHolder(method, args);
            return Proxy.newStatement(statementHolder, null, proxy, config, getExceptionListener());
        }
        if (methodName == "prepareStatement") { // *6
            ValueHolder<PreparedStatement> statementHolder =
                (ValueHolder<PreparedStatement>) getStatementHolder(method, args);
            return Proxy.newPreparedStatement(statementHolder, config.getStatementCache(), proxy, config,
                getExceptionListener());
        }
        if (methodName == "prepareCall") { // *3
            ValueHolder<CallableStatement> statementHolder =
                (ValueHolder<CallableStatement>) getStatementHolder(method, args);
            return Proxy.newCallableStatement(statementHolder, config.getStatementCache(), proxy, config,
                getExceptionListener());
        }
        if (methodName == "getMetaData") { // *1
            DatabaseMetaData metaData = (DatabaseMetaData) targetInvoke(method, args);
            return Proxy.newDatabaseMetaData(metaData, proxy, getExceptionListener());
        }

        return super.customInvoke(proxy, method, args);
    }

    private ValueHolder<? extends Statement> getStatementHolder(Method method, Object[] args) throws Throwable {
        if (statementCache != null) {
            Connection target = getTarget();
            StatementKey key = new StatementKey(target, method, args);
            ValueHolder<Statement> statementHolder = statementCache.get(key);
            if (statementHolder == null || statementHolder.inUse().getAndSet(true)) {
                Statement statement = (Statement) targetInvoke(method, args);
                if (statementHolder == null) { // there was no entry for the key, so we'll try to put a new one
                    statementHolder = new ValueHolder<Statement>(statement, new AtomicBoolean(true));
                    if (statementCache.putIfAbsent(key, statementHolder) != null)
                        // because another thread succeeded to put the entry before us
                        statementHolder = new ValueHolder<Statement>(statement, null);
                }
                return statementHolder;
            } else { // the statementHolder is valid and was not inUse
                if (logger.isTraceEnabled())
                    logger.trace("Using cached statement for connection {}, method {}, args {}",
                        target, method, Arrays.toString(args));
                return statementHolder;
            }
        } else {
            return getUncachedStatementHolder(method, args);
        }
    }

    private ValueHolder<? extends Statement> getUncachedStatementHolder(Method method, Object[] args) throws Throwable {
        Statement statement = (Statement) targetInvoke(method, args);
        return new ValueHolder<Statement>(statement, null);
    }

    private Object processCloseOrAbort(boolean isCloseMethod, Method method, Object[] args) throws Throwable {
        if (getAndSetClosed())
            return null;
        try {
            return isCloseMethod ? null : targetInvoke(method, args); // close() is not passed, abort() is passed
        } finally {
            boolean valid = isCloseMethod && getExceptionListener().getExceptions().isEmpty();
            connectionPool.restore(hConnection, valid);
        }
    }
}
