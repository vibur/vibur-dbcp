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
import vibur.dbcp.proxy.listener.ExceptionListener;
import vibur.dbcp.proxy.listener.TransactionListener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.*;

/**
 * @author Simeon Malchev
 */
class ConnectionChildInvocationHandler<T> extends AbstractInvocationHandler<T>
    implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionChildInvocationHandler.class);

    private final Connection connectionProxy;
    private final ViburDBCPConfig config;
    private final TransactionListener transactionListener;

    public ConnectionChildInvocationHandler(T connectionChild, Connection connectionProxy,
                                            ViburDBCPConfig config,
                                            TransactionListener transactionListener,
                                            ExceptionListener exceptionListener) {
        super(connectionChild, exceptionListener);
        if (connectionProxy == null || config == null || transactionListener == null)
            throw new NullPointerException();

        this.connectionProxy = connectionProxy;
        this.config = config;
        this.transactionListener = transactionListener;
    }

    protected Object customInvoke(T proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName.startsWith("execute"))
            return processExecute(proxy, method, args);

        if (methodName.equals("getConnection"))
            return connectionProxy;

        return super.customInvoke(proxy, method, args);
    }

    private Object processExecute(T proxy, Method method, Object[] args) throws Throwable {
        if (config.isLogStatementsEnabled())
            logger.debug("Executing SQL -> {}", proxy);

        long currentTime = 0;
        long queryExecuteTimeLimitInMs = config.getQueryExecuteTimeLimitInMs();
        if (queryExecuteTimeLimitInMs > 0)
            currentTime = System.currentTimeMillis();
        try {
            Object result = targetInvoke(method, args); // the real executeXXX call
            transactionListener.setInProgress(true);
            return result;
        } finally {
            if (queryExecuteTimeLimitInMs > 0) {
                long timeTaken = System.currentTimeMillis() - currentTime;
                if (timeTaken > queryExecuteTimeLimitInMs)
                    logger.debug("The execution of {} took {}ms", proxy, timeTaken);
            }
        }
    }
}
