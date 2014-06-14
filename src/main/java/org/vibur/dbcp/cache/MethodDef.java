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

package org.vibur.dbcp.cache;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Describes a {@code method} with {@code args} which has been invoked on a {@code target} object of type T.
 *
 * <p>Used as a caching {@code key} for method invocations in a {@link java.util.concurrent.ConcurrentMap}
 * cache implementation.
 *
 * @see StatementInvocationCacheProvider
 * @see ReturnVal
 *
 * @author Simeon Malchev
 * @param <T> the type of the object on which the method is invoked
 */
public class MethodDef<T> {

    private final T target;
    private final Method method;
    private final Object[] args;

    public MethodDef(T target, Method method, Object[] args) {
        if (target == null || method == null)
            throw new NullPointerException();
        this.target = target;
        this.method = method;
        this.args = args;
    }

    public T getTarget() {
        return target;
    }

    public Method getMethod() {
        return method;
    }

    public Object[] getArgs() {
        return args;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MethodDef that = (MethodDef) o;
        return target.equals(that.target)
            && method.equals(that.method)
            && Arrays.equals(args, that.args);
    }

    public int hashCode() {
        int result = target.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + Arrays.hashCode(args);
        return result;
    }
}
