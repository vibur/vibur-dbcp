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
import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.ViburDBCPException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.vibur.dbcp.ViburConfig.SQLSTATE_OBJECT_CLOSED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_WRAPPER_ERROR;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;

/**
 * @author Simeon Malchev
 * @param <T> the type of the object that we are dynamically proxy-ing
*/
abstract class AbstractInvocationHandler<T> implements TargetInvoker {

    private static final Logger logger = LoggerFactory.getLogger(AbstractInvocationHandler.class);

    /** The real (raw) object that we are dynamically proxy-ing.
     *  For example, the underlying JDBC Connection, the underlying JDBC Statement, etc. */
    private final T target;

    private final ViburConfig config;
    private final ExceptionCollector exceptionCollector;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    AbstractInvocationHandler(T target, ViburConfig config, ExceptionCollector exceptionCollector) {
        assert target != null;
        assert exceptionCollector != null;
        this.target = target;
        this.config = config;
        this.exceptionCollector = exceptionCollector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (logger.isTraceEnabled())
            logger.trace("Calling {} with args {} on {}", method, args, target);

        String methodName = method.getName();

        if (methodName == "equals") // comparing with == as the Method names are interned Strings
            return proxy == args[0];
        if (methodName == "hashCode")
            return System.identityHashCode(proxy);
        if (methodName == "toString")
            return "Proxy for: " + target;

        if (methodName == "unwrap")
            return unwrap((Class<T>) args[0]);
        if (methodName == "isWrapperFor")
            return isWrapperFor((Class<?>) args[0]);

        try {
            return doInvoke((T) proxy, method, args);
        } catch (ViburDBCPException e) {
            logger.error("Pool {}, the invocation of {} with args {} on {} threw:",
                    getPoolName(config), method, Arrays.toString(args), target, e);
            Throwable cause = e.getCause();
            if (cause instanceof SQLException)
                throw cause; // throw the original SQLException which have caused the ViburDBCPException
            throw e; // not expected to happen
        }
    }

    /**
     * By default, forwards the call to the original method of the proxied object. This method will be overridden
     * in the {@code AbstractInvocationHandler} subclasses, and will be the place to implement the specific to these
     * subclasses logic for methods invocation handling.
     *
     * @param proxy see {@link java.lang.reflect.InvocationHandler#invoke(Object, Method, Object[])}
     * @param method as above
     * @param args as above
     * @return as above
     * @throws Throwable as above
     */
    Object doInvoke(T proxy, Method method, Object[] args) throws Throwable {
        return targetInvoke(method, args);
    }

    @Override
    public final Object targetInvoke(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);  // the real method call on the real underlying (proxied) object
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause == null)
                cause = e;
            logInvokeFailure(method, args, cause);
            exceptionCollector.addException(cause);
            if (cause instanceof SQLException || cause instanceof RuntimeException || cause instanceof Error)
                throw cause;
            throw new ViburDBCPException(cause); // not expected to happen
        }
    }

    void logInvokeFailure(Method method, Object[] args, Throwable t) {
        if (logger.isDebugEnabled())
            logger.debug("Pool {}, the invocation of {} with args {} on {} threw:",
                    getPoolName(config), method, Arrays.toString(args), target, t);
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean getAndSetClosed() {
        return closed.getAndSet(true);
    }

    void ensureNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException(target.getClass().getName() + " is closed.", SQLSTATE_OBJECT_CLOSED_ERROR);
    }

    ExceptionCollector getExceptionCollector() {
        return exceptionCollector;
    }

    T getTarget() {
        return target;
    }

    private T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return target;
        throw new SQLException("not a wrapper for " + iface, SQLSTATE_WRAPPER_ERROR);
    }

    private boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(target);
    }
}
