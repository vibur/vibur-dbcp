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

import vibur.dbcp.proxy.listener.SQLExceptionListener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;

/**
 * @author Simeon Malchev
 */
public class ConnectionChildInvocationHandler<T> extends AbstractInvocationHandler<T>
    implements InvocationHandler {

    private final Connection connectionProxy;

    public ConnectionChildInvocationHandler(T connectionChild, Connection connectionProxy,
                                            SQLExceptionListener sqlExceptionListener) {
        super(connectionChild, sqlExceptionListener);
        if (connectionProxy == null)
            throw new NullPointerException();
        this.connectionProxy = connectionProxy;
    }

    protected Object customInvoke(T proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName.equals("getConnection"))
            return connectionProxy;

        return super.customInvoke(proxy, method, args);
    }
}
