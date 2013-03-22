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

import vibur.dbcp.ViburDBCPConfig;
import vibur.dbcp.proxy.listener.ExceptionListener;
import vibur.dbcp.proxy.listener.TransactionListener;
import vibur.object_pool.HolderValidatingPoolService;
import vibur.object_pool.Holder;

import java.lang.reflect.*;
import java.sql.*;

/**
 * @author Simeon Malchev
 */
public class Proxy {

    public static Connection newConnection(Holder<Connection> hConnection,
                                           HolderValidatingPoolService<Connection> connectionPool,
                                           ViburDBCPConfig config) {
        InvocationHandler handler = new ConnectionInvocationHandler(connectionPool, hConnection, config);
        return (Connection) newProxy(connectionCtor, handler);
    }

    public static Statement newStatement(Statement statement,
                                         Connection connectionProxy,
                                         ViburDBCPConfig config,
                                         TransactionListener transactionListener,
                                         ExceptionListener exceptionListener) {
        InvocationHandler handler = new ConnectionChildInvocationHandler<Statement>(
            statement, connectionProxy, config, transactionListener, exceptionListener);
        return (Statement) newProxy(statementCtor, handler);
    }

    public static PreparedStatement newPreparedStatement(PreparedStatement pStatement,
                                                         Connection connectionProxy,
                                                         ViburDBCPConfig config,
                                                         TransactionListener transactionListener,
                                                         ExceptionListener exceptionListener) {
        InvocationHandler handler = new ConnectionChildInvocationHandler<PreparedStatement>(
            pStatement, connectionProxy, config, transactionListener, exceptionListener);
        return (PreparedStatement) newProxy(pStatementCtor, handler);
    }

    public static CallableStatement newCallableStatement(CallableStatement cStatement,
                                                         Connection connectionProxy,
                                                         ViburDBCPConfig config,
                                                         TransactionListener transactionListener,
                                                         ExceptionListener exceptionListener) {
        InvocationHandler handler = new ConnectionChildInvocationHandler<CallableStatement>(
            cStatement, connectionProxy, config, transactionListener, exceptionListener);
        return (CallableStatement) newProxy(cStatementCtor, handler);
    }

    public static DatabaseMetaData newDatabaseMetaData(DatabaseMetaData metaData,
                                                       Connection connectionProxy,
                                                       ViburDBCPConfig config,
                                                       TransactionListener transactionListener,
                                                       ExceptionListener exceptionListener) {
        InvocationHandler handler = new ConnectionChildInvocationHandler<DatabaseMetaData>(
            metaData, connectionProxy, config, transactionListener, exceptionListener);
        return (DatabaseMetaData) newProxy(metadataCtor, handler);
    }

    private static Object newProxy(Constructor<?> proxyCtor, InvocationHandler invocationHandler) {
        try {
            return proxyCtor.newInstance(invocationHandler);
        } catch (IllegalArgumentException e) {
            throw new InternalError(e.toString());
        } catch (IllegalAccessException e) {
            throw new InternalError(e.toString());
        } catch (InstantiationException e) {
            throw new InternalError(e.toString());
        } catch (InvocationTargetException e) {
            throw new InternalError(e.toString());
        }
    }

    private static final Constructor<?> connectionCtor;
    private static final Constructor<?> statementCtor;
    private static final Constructor<?> pStatementCtor;
    private static final Constructor<?> cStatementCtor;
    private static final Constructor<?> metadataCtor;
    // static initializer for all constructors:
    static {
        try {
            ClassLoader classLoader = Proxy.class.getClassLoader();

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
        } catch (NoSuchMethodException e) {
            throw new InternalError(e.toString());
        }
    }
}
