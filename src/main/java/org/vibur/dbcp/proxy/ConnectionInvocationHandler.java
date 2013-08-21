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
import org.vibur.dbcp.cache.StatementKey;
import org.vibur.dbcp.cache.ValueHolder;
import org.vibur.dbcp.proxy.listener.ExceptionListenerImpl;
import org.vibur.dbcp.proxy.listener.TransactionListener;
import org.vibur.dbcp.proxy.listener.TransactionListenerImpl;
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

    private final HolderValidatingPoolService<Connection> connectionPool;
    private final Holder<Connection> hConnection;

    private final TransactionListener transactionListener;
    private final ViburDBCPConfig config;

    private volatile boolean autoCommit;
    private volatile boolean logicallyClosed = false;

    private final ConcurrentMap<StatementKey, ValueHolder<Statement>> statementCache;

    public ConnectionInvocationHandler(Holder<Connection> hConnection, ViburDBCPConfig config) {
        super(hConnection.value(), new ExceptionListenerImpl());
        HolderValidatingPoolService<Connection> connectionPool = config.getConnectionPool();
        if (connectionPool == null)
            throw new NullPointerException();
        this.connectionPool = connectionPool;
        this.hConnection = hConnection;
        this.transactionListener = new TransactionListenerImpl();
        this.config = config;
        this.autoCommit = config.getDefaultAutoCommit() != null ? config.getDefaultAutoCommit() : true;
        this.statementCache = config.getStatementCache();
    }

    @SuppressWarnings("unchecked")
    protected Object customInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        boolean isMethodNameClose = methodName.equals("close");
        if (isMethodNameClose || methodName.equals("abort"))
            return processCloseOrAbort(isMethodNameClose, method, args);

        if (methodName.equals("isClosed"))
            return logicallyClosed;

        // All other Connection interface methods cannot work if the JDBC Connection is closed:
        if (logicallyClosed)
            throw new SQLException("Connection is closed.");

        // Methods which results have to be proxied so that when getConnection() is called
        // on them the return value to be current JDBC Connection proxy.
        if (methodName.equals("createStatement")) { // *3
            ValueHolder<Statement> statementHolder =
                (ValueHolder<Statement>) getStatementHolder(method, args);
            return Proxy.newStatement(statementHolder, proxy,
                config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("prepareStatement")) { // *6
            ValueHolder<PreparedStatement> statementHolder =
                (ValueHolder<PreparedStatement>) getStatementHolder(method, args);
            return Proxy.newPreparedStatement(statementHolder, proxy,
                config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("prepareCall")) { // *3
            ValueHolder<CallableStatement> statementHolder =
                (ValueHolder<CallableStatement>) getStatementHolder(method, args);
            return Proxy.newCallableStatement(statementHolder, proxy,
                config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("getMetaData")) { // *1
            DatabaseMetaData metaData = (DatabaseMetaData) targetInvoke(method, args);
            return Proxy.newDatabaseMetaData(metaData, proxy, getExceptionListener());
        }

        if (methodName.equals("setAutoCommit")) {
            autoCommit = ((Boolean) args[0]);
            return targetInvoke(method, args);
        }
        if (methodName.equals("commit") || methodName.equals("rollback")) {
            transactionListener.setInProgress(false);
            return targetInvoke(method, args);
        }

        return super.customInvoke(proxy, method, args);
    }

    private ValueHolder<? extends Statement> getStatementHolder(Method method, Object[] args)
            throws Throwable {
        if (statementCache != null) {
            StatementKey key = new StatementKey(getTarget(), method, args);
            ValueHolder<Statement> statementHolder = statementCache.get(key);
            if (statementHolder == null || statementHolder.inUse().getAndSet(true)) {
                Statement statement = (Statement) targetInvoke(method, args);
                if (statementHolder == null) { // there was no entry for the key, so we'll try to put a new one
                    statementHolder = new ValueHolder<Statement>(statement, new AtomicBoolean(true));
                    if (statementCache.putIfAbsent(key, statementHolder) != null)
                        statementHolder = new ValueHolder<Statement>(statement, null); // because another thread succeeded to put the entry before us
                }
                return statementHolder;
            } else { // the statementHolder is valid and was not inUse
                logger.trace("Using cached statement for connection {}, method {}, args {}",
                    getTarget(), method, Arrays.toString(args));
                return statementHolder;
            }
        } else {
            Statement statement = (Statement) targetInvoke(method, args);
            return new ValueHolder<Statement>(statement, null);
        }
    }

    private Object processCloseOrAbort(boolean isClose, Method method, Object[] args) throws Throwable {
        logicallyClosed = true;
        boolean valid = isClose && getExceptionListener().getExceptions().isEmpty();

        if (isClose && !autoCommit && transactionListener.isInProgress()) {
            logger.error("Neither commit() nor rollback() were called before close(). Calling rollback() now.");
            try {
                hConnection.value().rollback();
            } catch (SQLException e) {
                logger.debug("Couldn't rollback the connection", e);
                if (!(e instanceof SQLTransientConnectionException))
                    valid = false;
            }
        }

        Object result = isClose ? null : targetInvoke(method, args); // close() is not passed, abort() is passed
        connectionPool.restore(hConnection, valid);
        return result;
    }
}
