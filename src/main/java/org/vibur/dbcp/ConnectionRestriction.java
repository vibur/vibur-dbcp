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

package org.vibur.dbcp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Describes the JDBC Connection restrictions in terms of what kind of SQL queries are allowed to be executed
 * on this connection. This could be a DDL only, a DML only, or a mixture of both.
 *
 * @see ViburDBCPDataSource#getRestrictedConnection(ConnectionRestriction)
 *
 * @author Simeon Malchev
 */
public enum ConnectionRestriction {

    WHITELISTED_DDL(ConnectionRestriction.SQL_DML_PREFIXES, true),
    BLACKLISTED_DDL(ConnectionRestriction.SQL_DML_PREFIXES, false);

    private static final Set<String> SQL_DML_PREFIXES = new HashSet<String>(Arrays.asList(
            "select", "insert", "update", "delete"));

    /**
     * If not {@code null}, will filter the attempted for execution SQL queries based on these prefixes.
     * The strings in this set must be in <b>all lower-case</b>.
     */
    private final Set<String> restrictedQueryPrefixes;

    /**
     * Will apply only if {@link #restrictedQueryPrefixes} is enabled. If set to {@code true}, the specified
     * restrictedQueryPrefixes will be treated as white-listed, otherwise as black-listed.
     */
    private final boolean whiteListed;

    ConnectionRestriction(Set<String> restrictedQueryPrefixes, boolean whiteListed) {
        this.restrictedQueryPrefixes = restrictedQueryPrefixes;
        this.whiteListed = whiteListed;
    }

    public Set<String> restrictedQueryPrefixes() {
        return restrictedQueryPrefixes;
    }

    public boolean whiteListed() {
        return whiteListed;
    }
}
