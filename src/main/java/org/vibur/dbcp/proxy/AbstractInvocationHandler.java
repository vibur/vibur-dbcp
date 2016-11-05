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
import org.vibur.dbcp.event.Hook;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.vibur.dbcp.ViburConfig.SQLSTATE_OBJECT_CLOSED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_WRAPPER_ERROR;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;

/**
 * @author Simeon Malchev
 * @param <T> the type of the object that we are dynamically proxying
*/
abstract class AbstractInvocationHandler<T> implements InvocationHandler, TargetInvoker {

    private static final Logger logger = LoggerFactory.getLogger(AbstractInvocationHandler.class);

    private static final Object NO_RESULT = new Object();

    /** The real (raw) object that we are dynamically proxying.
     *  For example, the underlying JDBC Connection, the underlying JDBC Statement, etc. */
    private final T target;

    private final List<Hook.MethodInvocation> onInvocation;
    private final ViburConfig config;
    private final ExceptionCollector exceptionCollector;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    AbstractInvocationHandler(T target, ViburConfig config, ExceptionCollector exceptionCollector) {
        assert target != null;
        assert config != null;
        assert exceptionCollector != null;
        this.target = target;
        this.onInvocation = config.getMethodHooks().onInvocation();
        this.config = config;
        this.exceptionCollector = exceptionCollector;
    }

    @Override
    public final Object invoke(Object objProxy, Method method, Object[] args) throws Throwable {
        if (logger.isTraceEnabled())
            logger.trace("Calling {} with args {} on {}", method, Arrays.toString(args), target);
        @SuppressWarnings("unchecked")
        T proxy = (T) objProxy;

        Object unrestrictedResult = unrestrictedInvoke(proxy, method, args); // (1)
        if (unrestrictedResult != NO_RESULT)
            return unrestrictedResult;

        restrictedAccessEntry(proxy, method, args); // (2)
        return restrictedInvoke(proxy, method, args); // (3)
    }

    /**
     * Handles all unrestricted method invocations that we can process before passing through the
     * {@link #restrictedAccessEntry}. This method will be overridden in the {@code AbstractInvocationHandler}
     * subclasses, and will be the place to implement the specific to these subclasses logic for unrestricted method
     * invocations handling. When the invoked {@code method} is not an unrestricted method the default implementation
     * returns {@link #NO_RESULT} to indicate this.
     *
     * @param proxy see {@link java.lang.reflect.InvocationHandler#invoke}
     * @param method as above
     * @param args as above
     * @return as above
     * @throws Throwable as above
     */
    Object unrestrictedInvoke(T proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (methodName == "equals") // comparing with == as the Method names are interned Strings
            return proxy == args[0];
        if (methodName == "hashCode")
            return System.identityHashCode(proxy);
        if (methodName == "toString")
            return "Vibur proxy for: " + target;
        // getClass(), notify(), notifyAll(), and wait() method calls are not intercepted by the dynamic proxies

        if (methodName == "unwrap") {
            @SuppressWarnings("unchecked")
            Class<T> iface = (Class<T>) args[0];
            return unwrap(iface);
        }
        if (methodName == "isWrapperFor")
            return isWrapperFor((Class<?>) args[0]);

        return NO_RESULT;
    }

    private void restrictedAccessEntry(T proxy, Method method, Object[] args) throws SQLException {
        if (isClosed())
            throw new SQLException(target.getClass().getName() + " is closed.", SQLSTATE_OBJECT_CLOSED_ERROR);
        for (Hook.MethodInvocation hook : onInvocation)
            hook.on(proxy, method, args);
    }

    /**
     * Handles all restricted method invocations that occur after (and if) we have passed through the
     * {@link #restrictedAccessEntry}. This method will be overridden in the {@code AbstractInvocationHandler}
     * subclasses, and will be the place to implement the specific to these subclasses logic for restricted method
     * invocations handling. The default implementation simply forwards the call to the original method of the
     * proxied object.
     *
     * @param proxy see {@link java.lang.reflect.InvocationHandler#invoke}
     * @param method as above
     * @param args as above
     * @return as above
     * @throws Throwable as above
     */
    Object restrictedInvoke(T proxy, Method method, Object[] args) throws Throwable {
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
            logTargetInvokeFailure(method, args, cause);
            exceptionCollector.addException(cause);
            if (cause instanceof SQLException || cause instanceof RuntimeException || cause instanceof Error)
                throw cause;
            logger.error("Unexpected exception cause", e);
            throw e; // not expected to happen
        }
    }

    void logTargetInvokeFailure(Method method, Object[] args, Throwable t) {
        if (logger.isDebugEnabled())
            logger.debug("Pool {}, the invocation of {} with args {} on {} threw:",
                    getPoolName(config), method, Arrays.toString(args), target, t);
    }

    /**
     * Logically closes this invocation handler. Returns true only once when the handler state transitions
     * from open to close.
     */
    final boolean close() {
        return !closed.getAndSet(true);
    }

    final boolean isClosed() {
        return closed.get();
    }

    final ExceptionCollector getExceptionCollector() {
        return exceptionCollector;
    }

    final T getTarget() {
        return target;
    }

    private T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return target;
        throw new SQLException("Not a wrapper for " + iface, SQLSTATE_WRAPPER_ERROR);
    }

    private boolean isWrapperFor(Class<?> iface) {
        return config.isAllowUnwrapping() && iface.isInstance(target);
    }
}
