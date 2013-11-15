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
import org.vibur.dbcp.proxy.listener.ExceptionListener;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
* @author Simeon Malchev
*/
public abstract class AbstractInvocationHandler<T> implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractInvocationHandler.class);

    /** The real object which we're dynamically proxy-ing.
     *  For example, the underlying JDBC Connection, the underlying JDBC Statement, etc. */
    private final T target;

    private final ExceptionListener exceptionListener;
    private final AtomicBoolean logicallyClosed = new AtomicBoolean(false);

    public AbstractInvocationHandler(T target, ExceptionListener exceptionListener) {
        if (target == null || exceptionListener == null)
            throw new NullPointerException();
        this.target = target;
        this.exceptionListener = exceptionListener;
    }

    /** @inheritDoc */
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (logger.isTraceEnabled())
            logger.trace("Calling {} with args {} on {}", method, args, target);
        String methodName = method.getName();

        if (methodName == "equals")
            return proxy == args[0];
        if (methodName == "hashCode")
            return System.identityHashCode(proxy);
        if (methodName == "toString")
            return "Proxy for: " + target;

        if (methodName == "unwrap")
            return unwrap((Class<T>) args[0]);
        if (methodName == "isWrapperFor")
            return isWrapperFor((Class<?>) args[0]);

       return customInvoke((T) proxy, method, args);
    }

    @SuppressWarnings("unchecked")
    private <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return (T) target;
        throw new SQLException("not a wrapper for " + iface);
    }

    private boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(target);
    }

    protected Object customInvoke(T proxy, Method method, Object[] args) throws Throwable {
        // by default we just pass the call to the proxied object method
        return targetInvoke(method, args);
    }

    protected Object targetInvoke(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);  // the real method call on the real underlying (proxied) object
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof SQLTransientConnectionException)) // transient exceptions are not remembered
                exceptionListener.addException(cause);
            throw cause;
        }
    }

    protected boolean isClosed() {
        return logicallyClosed.get();
    }

    protected boolean getAndSetClosed() {
        return logicallyClosed.getAndSet(true);
    }

    protected void ensureNotClosed() throws SQLException {
        if (logicallyClosed.get())
            throw new SQLException(target.getClass().getName() + " is closed.");
    }

    protected ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    protected T getTarget() {
        return target;
    }
}
