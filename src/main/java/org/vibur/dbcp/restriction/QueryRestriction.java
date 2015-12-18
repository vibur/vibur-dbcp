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
 * Defines the SQL query restrictions. This could be a DDL queries only, a DML only, or a mixture of both.
 *
 * @see ConnectionRestriction
 * @see QueryRestrictions
 *
 * @author Simeon Malchev
 */
public interface QueryRestriction {

    /**
     * Returns a set of String prefixes that will be used to filter (allow or forbid) the attempted SQL queries.
     * The strings in this set must be in <b>all lower-case</b>, without leading or trailing whitespaces,
     * and must consist of one or two words only. When there are two words, they must be separated by a single space.
     * For examples, see {@link QueryRestrictions}.
     * <p>
     * Returning {@code null} means there are no restrictions. Returning an empty set will effectively
     * forbid all SQL queries.
     *
     * @return see above
     */
    Set<String> restrictedPrefixes();

    /**
     * Will apply only if {@link #restrictedPrefixes()} returns a non-null value. If set to {@code true},
     * the specified restricted query prefixes will be treated as being white-listed, otherwise as black-listed.
     *
     * @return see above
     */
    boolean whiteListed();
}
