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

package vibur.dbcp.proxy.cache;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Arrays;

/**
 * @author Simeon Malchev
 */
public class StatementKey {

    private final Connection proxy;
    private final Method method;
    private final Object[] args;

    public StatementKey(Connection proxy, Method method, Object[] args) {
        if (proxy == null || method == null || args == null)
            throw new NullPointerException();
        this.proxy = proxy;
        this.method = method;
        this.args = args;
    }

    public Connection getProxy() {
        return proxy;
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

        StatementKey that = (StatementKey) o;
        return proxy.equals(that.proxy)
            && method.equals(that.method)
            && Arrays.equals(args, that.args);
    }

    public int hashCode() {
        int result = proxy.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + Arrays.hashCode(args);
        return result;
    }
}
