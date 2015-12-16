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

package org.vibur.dbcp.util.collector;

import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * JDBC proxy objects exceptions collector - operations implementation.
 *
 * <p><strong>Note that</strong> this class plays an important role when Vibur DBCP needs to decide whether
 * to physically close the raw JDBC Connection which corresponding Connection proxy object has been just closed.
 * An application class that wants to receive notifications for all thrown on a JDBC Connection object (and its
 * derivatives) exceptions, may sub-class this class and override the {@link #addException(Throwable)} method,
 * returning the call back to its super-method.
 *
 * @author Simeon Malchev
 */
public class BaseExceptionCollector implements ExceptionCollector {

    private final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<Throwable>();

    /** {@inheritDoc} */
    public void addException(Throwable t) {
        if (!(t instanceof SQLTransientException) || t instanceof SQLTransientConnectionException)
            exceptions.add(t); // the above SQL transient exceptions are not of interest and are ignored
    }

    /** {@inheritDoc} */
    public List<Throwable> getExceptions() {
        return exceptions.isEmpty() ? Collections.<Throwable>emptyList() : new LinkedList<Throwable>(exceptions);
    }
}
