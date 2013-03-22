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

package vibur.dbcp.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vibur.dbcp.ViburDBCPConfig;
import vibur.dbcp.cache.ConcurrentCache;
import vibur.dbcp.proxy.listener.ExceptionListenerImpl;
import vibur.dbcp.proxy.listener.TransactionListener;
import vibur.dbcp.proxy.listener.TransactionListenerImpl;
import vibur.object_pool.Holder;
import vibur.object_pool.HolderValidatingPoolService;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.*;

/**
 * @author Simeon Malchev
 */
class ConnectionInvocationHandler extends AbstractInvocationHandler<Connection>
    implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionInvocationHandler.class);

    private final HolderValidatingPoolService<Connection> connectionPool;
    private final Holder<Connection> hConnection;

    private final TransactionListener transactionListener;
    private final ViburDBCPConfig config;

    private volatile boolean autoCommit;
    private volatile boolean logicallyClosed = false;

    private final ConcurrentCache<StatementDescriptor, Statement> statementCache;

    public ConnectionInvocationHandler(HolderValidatingPoolService<Connection> connectionPool,
                                       Holder<Connection> hConnection, ViburDBCPConfig config) {
        super(hConnection.value(), new ExceptionListenerImpl());
        if (connectionPool == null || config == null)
            throw new NullPointerException();

        this.connectionPool = connectionPool;
        this.hConnection = hConnection;
        this.transactionListener = new TransactionListenerImpl();
        this.config = config;
        this.autoCommit = config.getDefaultAutoCommit() != null ? config.getDefaultAutoCommit() : false;
        this.statementCache = config.getStatementCache();
    }

    protected Object customInvoke(Connection proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        boolean isMethodNameClose = methodName.equals("close");
        if (isMethodNameClose || methodName.equals("abort"))
            return processCloseOrAbort(isMethodNameClose);

        if (methodName.equals("isClosed"))
            return logicallyClosed;

        // All other Connection interface methods cannot work if the JDBC Connection is closed:
        if (logicallyClosed)
            throw new SQLException("Connection is closed.");

        // Methods which results have to be proxied so that when getConnection() is called
        // on them the return value to be current JDBC Connection proxy.
        if (methodName.equals("createStatement")) { // *3
            Statement statement = getStatement(method, args);
            return Proxy.newStatement(statement, proxy, config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("prepareStatement")) { // *6
            PreparedStatement pStatement = (PreparedStatement) getStatement(method, args);
            return Proxy.newPreparedStatement(pStatement, proxy, config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("prepareCall")) { // *3
            CallableStatement cStatement = (CallableStatement) getStatement(method, args);
            return Proxy.newCallableStatement(cStatement, proxy, config, transactionListener, getExceptionListener());
        }
        if (methodName.equals("getMetaData")) { // *1
            DatabaseMetaData metaData = (DatabaseMetaData) targetInvoke(method, args);
            return Proxy.newDatabaseMetaData(metaData, proxy, config, transactionListener, getExceptionListener());
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

    private Statement getStatement(Method method, Object[] args) throws Throwable {
        Statement statement;
        if (statementCache != null) {
            StatementDescriptor descriptor = new StatementDescriptor(getTarget(), method, args);
            statement = statementCache.get(descriptor);
            if (statement == null) {
                statement = (Statement) targetInvoke(method, args);
                statementCache.putIfAbsent(descriptor, statement);
            }
        } else
            statement = (Statement) targetInvoke(method, args);
        return statement;
    }

    private Object processCloseOrAbort(boolean isClose) {
        logicallyClosed = true;
        boolean valid = isClose && getExceptionListener().getExceptions().isEmpty();

        if (!autoCommit && transactionListener.isInProgress()) {
            if (isClose)
                logger.error("Neither commit() nor rollback() were called before close()");
            logger.debug("Calling rollback() now");
            try {
                hConnection.value().rollback();
            } catch (SQLException e) {
                logger.debug("Couldn't rollback the connection", e);
                valid = false;
            }
        }

        connectionPool.restore(hConnection, valid);
        return null; // don't pass the close() or abort() calls
    }
}
