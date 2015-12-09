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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;

/**
 * An utility enum which contains several pre-defined implementations of the {@link QueryRestriction}
 * interface.
 *
 * @author Simeon Malchev
 */
public enum QueryRestrictions implements QueryRestriction {

    WHITELISTED_DML,
    BLACKLISTED_DML,
    SELECT_ONLY,
    BLACKLISTED_ROLE_SCHEMA;

    private static final Set<String> SQL_DML_PREFIXES = unmodifiableSet(new HashSet<String>(Arrays.asList(
            "select", "insert", "update", "delete")));
    private static final Set<String> SELECT_PREFIX = singleton("select");
    private static final Set<String> SET_ROLE_SCHEMA = unmodifiableSet(new HashSet<String>(Arrays.asList(
            "set role", "reset role", "set schema", "reset schema", "set search_path", "reset search_path")));

    static {
        WHITELISTED_DML.set(SQL_DML_PREFIXES, true);
        BLACKLISTED_DML.set(SQL_DML_PREFIXES, false);
        SELECT_ONLY.set(SELECT_PREFIX, true);
        BLACKLISTED_ROLE_SCHEMA.set(SET_ROLE_SCHEMA, false);
    }

    private Set<String> restrictedPrefixes;
    private boolean whiteListed;

    private void set(Set<String> restrictedQueryPrefixes, boolean whiteListed) {
        this.restrictedPrefixes = restrictedQueryPrefixes;
        this.whiteListed = whiteListed;
    }

    /** {@inheritDoc} */
    public Set<String> restrictedPrefixes() {
        return restrictedPrefixes;
    }

    /** {@inheritDoc} */
    public boolean whiteListed() {
        return whiteListed;
    }
}
