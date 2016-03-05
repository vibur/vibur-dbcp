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

import org.vibur.dbcp.ViburDBCPConfig;

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
 * @author Simeon Malchev
 */
public class ExceptionCollectorImpl implements ExceptionCollector {

    private final ViburDBCPConfig config;

    private final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<Throwable>();

    public ExceptionCollectorImpl(ViburDBCPConfig config) {
        if (config == null)
            throw new NullPointerException();
        this.config = config;
    }

    @Override
    public void addException(Throwable t) {
        if (config.getExceptionListener() != null)
            config.getExceptionListener().on(t);

        if (!(t instanceof SQLTransientException) || t instanceof SQLTransientConnectionException)
            exceptions.add(t); // the above SQL transient exceptions are not of interest and are ignored
    }

    @Override
    public List<Throwable> getExceptions() {
        return exceptions.isEmpty() ? Collections.<Throwable>emptyList() : new LinkedList<Throwable>(exceptions);
    }
}
