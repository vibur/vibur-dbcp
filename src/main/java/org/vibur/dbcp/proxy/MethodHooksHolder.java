/**
 * Copyright 2016 Simeon Malchev
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

import org.vibur.dbcp.event.Hook;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

/**
 * Holds all programming method invocation hooks collections.
 *
 * <p>Note that the underlying data structures used to store the Hook instances <b>are not</b> thread-safe
 * for modifications.
 *
 * @author Simeon Malchev
 */
public class MethodHooksHolder {

    /** A list of programming {@linkplain Hook.MethodInvocation#on hooks} intercepting (almost) all method calls on all
     * proxied JDBC interfaces. Methods inherited from the {@link Object} class, methods related to the "closed" state
     * of the JDBC objects (e.g., close(), isClosed()), as well as methods from the {@link java.sql.Wrapper} interface
     * are not intercepted. The hooks execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.MethodInvocation> onInvocation = EMPTY_LIST;

    public void addOnInvocation(Hook.MethodInvocation hook) {
        if (onInvocation == EMPTY_LIST)
            onInvocation = new ArrayList<>();
        onInvocation.add(hook);
    }

    List<Hook.MethodInvocation> onInvocation() {
        return onInvocation;
    }
}
