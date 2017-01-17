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

import org.vibur.dbcp.ViburConfig;

import java.lang.reflect.Method;

/**
 * @author Simeon Malchev
 * @param <P> the type of the parent object from which the child {@code T} object was derived
 * @param <T> the type of the child object that we are dynamically proxying
 */
class ChildObjectInvocationHandler<P, T> extends AbstractInvocationHandler<T> {

    private final P parentProxy;
    private final String getParentMethod;

    ChildObjectInvocationHandler(T targetChild, P parentProxy, String getParentMethod,
                                 ViburConfig config, ExceptionCollector exceptionCollector) {
        super(targetChild, config, exceptionCollector);
        assert parentProxy != null;
        assert getParentMethod != null;
        this.parentProxy = parentProxy;
        this.getParentMethod = getParentMethod;
    }

    @Override
    Object restrictedInvoke(T proxy, Method method, Object[] args) throws Throwable {
        if (method.getName() == getParentMethod)
            return parentProxy;

        return super.restrictedInvoke(proxy, method, args);
    }
}
