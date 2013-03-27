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
import vibur.dbcp.proxy.cache.StatementDescriptor;
import vibur.dbcp.proxy.cache.StatementKey;
import vibur.dbcp.proxy.listener.ExceptionListener;
import vibur.dbcp.proxy.listener.TransactionListener;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Simeon Malchev
 */
public class StatementInvocationHandler extends ConnectionChildInvocationHandler<Statement> {

    private static final Logger logger = LoggerFactory.getLogger(StatementInvocationHandler.class);

    private final StatementKey statementKey;
    private final ViburDBCPConfig config;
    private final TransactionListener transactionListener;

    private final ConcurrentCache<StatementKey, Statement> statementCache;

    private volatile boolean logicallyClosed = false;
    private volatile boolean closeOnCompletion = false;

    public StatementInvocationHandler(StatementDescriptor statementDescriptor,
                                      Connection connectionProxy,
                                      ViburDBCPConfig config,
                                      TransactionListener transactionListener,
                                      ExceptionListener exceptionListener) {
        super(statementDescriptor.getStatement(), connectionProxy, exceptionListener);
        if (config == null || transactionListener == null)
            throw new NullPointerException();
        this.statementKey = statementDescriptor.getStatementKey();
        this.config = config;
        this.transactionListener = transactionListener;
        this.statementCache = config.getStatementCache();
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName.equals("close")) {
            logicallyClosed = true;
            if (statementCache != null && statementKey != null)
                statementCache.restore(statementKey);
            return null;
        }
        if (methodName.equals("isClosed"))
            return logicallyClosed;

        // All other Statement interface methods cannot work if the JDBC Statement is closed:
        if (logicallyClosed)
            throw new SQLException("Statement is closed.");

        if (methodName.startsWith("execute"))
            return processExecute((Statement) proxy, method, args);

        if (methodName.equals("closeOnCompletion")) {
            closeOnCompletion = true;
            return null;
        }
        if (methodName.equals("isCloseOnCompletion"))
            return closeOnCompletion;

        return super.invoke(proxy, method, args);
    }

    private Object processExecute(Statement statementProxy, Method method, Object[] args) throws Throwable {
        if (config.isLogStatementsEnabled())
            logger.debug("Executing SQL -> {}", statementProxy);

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
                    logger.debug("The execution of {} took {}ms", statementProxy, timeTaken);
            }
        }
    }
}
