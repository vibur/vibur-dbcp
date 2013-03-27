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

import vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
* @author Simeon Malchev
*/
public abstract class AbstractInvocationHandler<T> implements InvocationHandler {

    /** The real object which we're dynamically proxy-ing.
     *  For example, the underlying JDBC Connection, the underlying JDBC Statement, etc. */
    private final T target;
    private final ExceptionListener exceptionListener;

    public AbstractInvocationHandler(T target, ExceptionListener exceptionListener) {
        if (target == null || exceptionListener == null)
            throw new NullPointerException();

        this.target = target;
        this.exceptionListener = exceptionListener;
    }

    /** @inheritDoc */
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName.equals("equals"))
            return proxy == args[0];
        if (methodName.equals("hashCode"))
            return System.identityHashCode(proxy);
        if (methodName.equals("toString"))
            return "Proxy for: " + target;

        // by default we just pass the call to the proxied object method
        return targetInvoke(method, args);
    }

    protected Object targetInvoke(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            exceptionListener.addException(cause);
            throw cause;
        }
    }

    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    public T getTarget() {
        return target;
    }
}
