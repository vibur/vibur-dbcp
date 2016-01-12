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

package org.vibur.dbcp.util;

import org.vibur.dbcp.restriction.ConnectionRestriction;
import org.vibur.dbcp.restriction.QueryRestriction;

import java.util.Set;

/**
 * @author Simeon Malchev
 */
public abstract class AbstractConnectionRestriction implements ConnectionRestriction {

    @Override
    public QueryRestriction getQueryRestriction() {
        return null;
    }

    @Override
    public Set<String> getForbiddenMethods() {
        return null;
    }

    @Override
    public boolean allowsUnwrapping() {
        return true;
    }
}
