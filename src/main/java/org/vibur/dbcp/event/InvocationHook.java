/**
 * Copyright 2016 Simeon Malchev
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

package org.vibur.dbcp.event;

import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * An application programming hook allowing us to intercept all method calls on all proxied JDBC interfaces.
 *
 * @author Simeon Malchev
 */
public interface InvocationHook {

    /**
     * An application hook that will be invoked when a method on any of the proxied JDBC interfaces is invoked.
     * The execution of this method should take as short time as possible.
     *
     * @param proxy the proxy instance that the method was invoked on
     * @param method the invoked method
     * @param args the method arguments
     * @throws SQLException to indicate that an error has occured
     */
    void invoke(Object proxy, Method method, Object[] args) throws SQLException;
}
