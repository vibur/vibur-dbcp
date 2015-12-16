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

import java.util.List;

/**
 * JDBC proxy objects exceptions collector - operations definitions.
 *
 * <p>The purpose of this collector is to accumulate all non-transient SQL exceptions that has been thrown by the
 * operations invoked on a JDBC Connection object or any of its direct or indirect derivative objects, such as
 * Statement, ResultSet, or database Metadata objects.
 *
 * @see BaseExceptionCollector
 *
 * @author Simeon Malchev
 */
public interface ExceptionCollector {

    /**
     * This method will be called by Vibur DBCP when an operation invoked on a JDBC Connection object or any of its
     * direct or indirect derivative objects, such as Statement, ResultSet, or database Metadata objects, throws
     * an Exception. If needed, this method may implement filtering logic and can store/accumulate only those
     * Exceptions that are perceived as being not-temporarily.
     *
     * @param throwable the exception thrown
     */
    void addException(Throwable throwable);

    /**
     * Returns a list of all exceptions that were filtered and accumulated by the {@link #addException(Throwable)}
     * method. This method will be called by Vibur DBCP when a JDBC Connection proxy has been closed, in order to
     * determine whether the underlying (raw) JDBC Connection needs to be physically closed, too.
     *
     * @return see above
     */
    List<Throwable> getExceptions();
}
