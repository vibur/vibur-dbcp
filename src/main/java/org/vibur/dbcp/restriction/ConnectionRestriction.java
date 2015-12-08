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

package org.vibur.dbcp.restriction;

import java.util.Set;

/**
 * Defines the JDBC Connection restrictions. The restrictions may include query restrictions, forbidden methods
 * from the {@link java.sql.Connection} interface, forbidden JDBC objects unwrapping.
 * <p>
 * The invocation of any of the forbidden methods or an attempt to execute a forbidden SQL query, will result
 * in an SQLException being thrown.
 *
 * @see QueryRestriction
 *
 * @author Simeon Malchev
 */
public interface ConnectionRestriction {

    /**
     * Returns the query restrictions. Returning {@code null} means there are no restrictions.
     * @return see above
     */
    QueryRestriction getQueryRestriction();

    /**
     * Returns a set of Strings containing the names of forbidden for invocation methods from the
     * {@link java.sql.Connection} interface. Returning {@code null} or an empty set means there are no
     * forbidden methods.
     * @return see above
     */
    Set<String> getForbiddenMethods();

    /**
     * Returning {@code true} means that the JDBC objects unwrapping is allowed.
     * @return see above
     */
    boolean allowsUnwrapping();
}
