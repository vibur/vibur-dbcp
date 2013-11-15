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

import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @author Simeon Malchev
 */
public class ChildObjectInvocationHandler<P, T> extends AbstractInvocationHandler<T>
    implements InvocationHandler {

    private final P parentProxy;
    private final String getParentMethod;

    public ChildObjectInvocationHandler(T connectionChild, P parentProxy, String getParentMethod,
                                        ExceptionListener exceptionListener) {
        super(connectionChild, exceptionListener);
        if (parentProxy == null || getParentMethod == null)
            throw new NullPointerException();
        this.parentProxy = parentProxy;
        this.getParentMethod = getParentMethod;
    }

    protected Object customInvoke(T proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == getParentMethod)
            return parentProxy;

        return super.customInvoke(proxy, method, args);
    }
}
