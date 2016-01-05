/**
 * Copyright 2015 Simeon Malchev
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

package org.vibur.dbcp.util.listener;

/**
 * JDBC proxy objects exceptions listener.
 *
 * <p>This listener will receive notifications for all exceptions thrown by the operations
 * invoked on a JDBC Connection object or any of its direct or indirect derivative objects
 * (such as Statement, ResultSet, or database Metadata objects).
 *
 * @author Simeon Malchev
 */
public interface ExceptionListener {

    /**
     * This method will be called when an Exception is thrown by a method invoked on a
     * proxied JDBC Connection object or any of its direct or indirect derivative objects.
     *
     * @param t the thrown exception. Most often this will be an SQLException, but may also
     *          be a RuntimeException or an Error.
     */
    void on(Throwable t);
}
