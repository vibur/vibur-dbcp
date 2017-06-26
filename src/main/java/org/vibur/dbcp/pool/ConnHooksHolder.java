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

package org.vibur.dbcp.pool;

import javax.sql.DataSource;

import static org.vibur.dbcp.pool.DefaultHook.Util.addHook;

/**
 * Holds all programming Connection hooks collections.
 *
 * <p>Note that the underlying data structures used to store the Hook instances <b>are not</b> thread-safe
 * for modifications. They must be set only once during the pool configuration phase and must not be modified after
 * the pool is started.
 *
 * @author Simeon Malchev
 */
public class ConnHooksHolder {

    /** A list of programming {@linkplain Hook.InitConnection#on hooks} that will be invoked only once <i>after</i>
     * the raw JDBC Connection is first created. Their execution should take as short time as possible. */
    private Hook.InitConnection[] onInit = new Hook.InitConnection[0];

    /** A list of programming {@linkplain Hook.GetConnection#on hooks} that will be invoked on the raw JDBC Connection
     * <i>after</i> it was taken from the pool as part of the {@link DataSource#getConnection()} flow.
     * Their execution should take as short time as possible. */
    private Hook.GetConnection[] onGet = new Hook.GetConnection[0];

    /** A list of programming {@linkplain Hook.CloseConnection#on hooks} that will be invoked on the raw JDBC Connection
     * <i>before</i> it is restored back to the pool as part of the {@link java.sql.Connection#close()} flow.
     * Their execution should take as short time as possible. */
    private Hook.CloseConnection[] onClose = new Hook.CloseConnection[0];

    /** A list of programming {@linkplain Hook.DestroyConnection#on hooks} that will be invoked only once <i>after</i>
     * the raw JDBC Connection is closed/destroyed. Their execution should take as short time as possible. */
    private Hook.DestroyConnection[] onDestroy = new Hook.DestroyConnection[0];

    public void addOnInit(Hook.InitConnection hook) {
        onInit = addHook(onInit, hook);
    }

    public void addOnGet(Hook.GetConnection hook) {
        onGet = addHook(onGet, hook);
    }

    public void addOnClose(Hook.CloseConnection hook) {
        onClose = addHook(onClose, hook);
    }

    public void addOnDestroy(Hook.DestroyConnection hook) {
        onDestroy = addHook(onDestroy, hook);
    }

    Hook.InitConnection[] onInit() {
        return onInit;
    }

    Hook.GetConnection[] onGet() {
        return onGet;
    }

    Hook.CloseConnection[] onClose() {
        return onClose;
    }

    Hook.DestroyConnection[] onDestroy() {
        return onDestroy;
    }
}
