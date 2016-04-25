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

import org.vibur.dbcp.ViburDBCPConfig;

import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The purpose of this collector is to receive notifications for all exceptions thrown by the
 * operations invoked on a JDBC Connection object or any of its direct or indirect derivative objects
 * (such as Statement, ResultSet, or database Metadata objects), and to accumulate a list of all
 * non-transient exceptions.
 *
 * @author Simeon Malchev
 */
class ExceptionCollector {

    private final ViburDBCPConfig config;

    private final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    ExceptionCollector(ViburDBCPConfig config) {
        this.config = config;
    }

    /**
     * This method will be called by Vibur DBCP when an operation invoked on a JDBC Connection object or any of its
     * direct or indirect derivative objects, such as Statement, ResultSet, or database Metadata objects, throws
     * an Exception. If needed, this method may implement filtering logic and can store/accumulate only those
     * Exceptions that are considered non-transient.
     *
     * @param t the exception thrown
     */
    void addException(Throwable t) {
        if (config.getExceptionListener() != null)
            config.getExceptionListener().on(t);

        if (!(t instanceof SQLTransientException) || t instanceof SQLTransientConnectionException)
            exceptions.add(t); // the above SQL transient exceptions are not of interest and are ignored
    }

    /**
     * Returns a list of all exceptions that were filtered and accumulated by the {@link #addException(Throwable)}
     * method. This method will be called by Vibur DBCP when a JDBC Connection proxy has been closed, in order to
     * determine whether the underlying (raw) JDBC Connection needs to be physically closed or not.
     *
     * @return see above
     */
    List<Throwable> getExceptions() {
        return exceptions.isEmpty() ? Collections.<Throwable>emptyList() : new LinkedList<>(exceptions);
    }
}
