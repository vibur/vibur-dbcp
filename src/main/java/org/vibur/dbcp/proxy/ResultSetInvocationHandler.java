/**
 * Copyright 2015 Simeon Malchev
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

import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Simeon Malchev
 */
public class ResultSetInvocationHandler extends ChildObjectInvocationHandler<Statement, ResultSet> {

    private final AtomicInteger resultSetSize;

    public ResultSetInvocationHandler(ResultSet rawResultSet, Statement statementProxy,
                                      AtomicInteger resultSetSize, ExceptionListener exceptionListener) {
        super(rawResultSet, statementProxy, "getStatement", exceptionListener);
        this.resultSetSize = resultSetSize;
    }

    protected Object doInvoke(ResultSet proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "next")
            resultSetSize.incrementAndGet();

        return super.doInvoke(proxy, method, args);
    }
}
