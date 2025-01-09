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
import org.vibur.dbcp.pool.Hook;
import org.vibur.dbcp.pool.HookHolder.InvocationHooksAccessor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.vibur.dbcp.ViburConfig.SQLSTATE_OBJECT_CLOSED_ERROR;
import static org.vibur.dbcp.ViburConfig.SQLSTATE_WRAPPER_ERROR;
import static org.vibur.dbcp.util.ViburUtils.getPoolName;

/**
 * @author Simeon Malchev
 * @param <T> the type of the object that we are dynamically proxying
*/
abstract class AbstractInvocationHandler<T> extends ExceptionCollector implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractInvocationHandler.class);

    private static final Object NO_RESULT = new Object();

    /** The real (raw) object that we are dynamically proxying.
     *  For example, the underlying JDBC Connection, the underlying JDBC Statement, etc. */
    private final T target;

    private final ViburConfig config;
    private final Hook.MethodInvocation[] onMethodInvocation;

    private final ExceptionCollector exceptionCollector;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    AbstractInvocationHandler(T target, ViburConfig config, ExceptionCollector exceptionCollector) {
        assert target != null;
        assert config != null;
        this.target = target;
        this.config = config;
        this.onMethodInvocation = ((InvocationHooksAccessor) config.getInvocationHooks()).onMethodInvocation();
        // not every AbstractInvocationHandler (this) is an ExceptionCollector
        this.exceptionCollector = exceptionCollector == null ? this : exceptionCollector;
    }

    @Override
    public final Object invoke(Object objProxy, Method method, Object[] args) throws SQLException {
        if (logger.isTraceEnabled()) {
            logger.trace("Calling {} with args {} on {}", method, Arrays.toString(args), target);
        }
        @SuppressWarnings("unchecked")
        var proxy = (T) objProxy;

        Object unrestrictedResult;
        if (!method.getName().startsWith("get") && // shortcuts getXYZ methods
            (unrestrictedResult = unrestrictedInvoke(proxy, method, args)) != NO_RESULT) { // (1)
            return unrestrictedResult;
        }

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
     * @throws SQLException if the invoked underlying method throws such
     */
    Object unrestrictedInvoke(T proxy, Method method, Object[] args) throws SQLException {
        var methodName = method.getName();

        if (methodName == "equals") { // comparing with == as the Method names are interned Strings
            return proxy == args[0];
        }
        if (methodName == "hashCode") {
            return System.identityHashCode(proxy);
        }
        if (methodName == "toString") {
            return "Vibur proxy for: " + target;
        }
        // getClass(), notify(), notifyAll(), and wait() method calls are not intercepted by the dynamic proxies

        if (methodName == "unwrap") {
            @SuppressWarnings("unchecked")
            var iface = (Class<T>) args[0];
            return unwrap(iface);
        }
        if (methodName == "isWrapperFor") {
            return isWrapperFor((Class<?>) args[0]);
        }

        return NO_RESULT;
    }

    private void restrictedAccessEntry(T proxy, Method method, Object[] args) throws SQLException {
        if (isClosed()) {
            throw new SQLException(target.getClass().getName() + " is closed.", SQLSTATE_OBJECT_CLOSED_ERROR);
        }
        for (var hook : onMethodInvocation) {
            hook.on(proxy, method, args);
        }
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
     * @throws SQLException if the invoked underlying method throws such
     */
    Object restrictedInvoke(T proxy, Method method, Object[] args) throws SQLException {
        return targetInvoke(method, args);
    }

    final Object targetInvoke(Method method, Object[] args) throws SQLException {
        try {
            return method.invoke(target, args);  // the real method call on the real underlying (proxied) object

        } catch (ReflectiveOperationException e) {
            throw underlyingException(method, args, e);
        }
    }

    private SQLException underlyingException(Method method, Object[] args, ReflectiveOperationException e) {
        if (e instanceof IllegalAccessException) {
            throw unexpectedException(e);
        }

        var cause = e.getCause() != null ? e.getCause() : e;
        if (logger.isDebugEnabled()) {
            logger.debug("Pool {}, the invocation of {} with args {} on {} threw:",
                    getPoolName(config), method, Arrays.toString(args), target, cause);
        }

        if (cause instanceof SQLException) {
            var sqlException = (SQLException) cause;
            exceptionCollector.addException(sqlException);
            return sqlException;
        }

        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }

        throw unexpectedException(e);
    }

    private static ViburDBCPException unexpectedException(ReflectiveOperationException e) {
        logger.error("Unexpected exception cause", e);
        return new ViburDBCPException(e); // not expected to happen
    }

    /**
     * Logically closes this invocation handler. Returns true only once when the InvocationHandler state changes
     * from opened to closed.
     */
    final boolean close() {
        return !closed.getAndSet(true);
    }

    final boolean isClosed() {
        return closed.get();
    }

    final T getTarget() {
        return target;
    }

    private T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return target;
        }
        throw new SQLException("Not a wrapper or unwrapping is disabled for " + iface, SQLSTATE_WRAPPER_ERROR);
    }

    private boolean isWrapperFor(Class<?> iface) {
        return config.isAllowUnwrapping() && iface.isInstance(target);
    }
}
