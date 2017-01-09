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

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This collector will receive notifications for all exceptions thrown by the operations invoked on a JDBC
 * Connection object or any of its direct or indirect derivative objects (such as Statement, ResultSet,
 * or database Metadata objects).
 *
 * @author Simeon Malchev
 */
class ExceptionCollector {

    private final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    /**
     * This method will be called by when an operation invoked on a JDBC object throws an Exception.
     * It will accumulate a list of all non-transient exceptions.
     *
     * @param t the exception thrown
     */
    void addException(Throwable t) {
        if (t instanceof SQLException
                && !(t instanceof SQLTimeoutException) && !(t instanceof SQLTransactionRollbackException))
            exceptions.offer(t); // only SQLExceptions are stored, excluding the above two sub-types
    }

    /**
     * Returns a list of all collected by {@link #addException} exceptions. This method will be
     * called when a pooled Connection is closed, in order to determine whether the underlying
     * (raw) JDBC Connection needs also to be closed or not.
     */
    List<Throwable> getExceptions() {
        return exceptions.isEmpty() ? Collections.<Throwable>emptyList() : new ArrayList<>(exceptions);
    }
}
