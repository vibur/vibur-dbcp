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

import org.vibur.dbcp.event.Hook;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

/**
 * Holds all programming Connection hooks collections.
 *
 * <p>Note that the underlying data structures used to store the Hook instances <b>are not</b> thread-safe
 * for modifications.
 *
 * @author Simeon Malchev
 */
public class ConnHooksHolder {

    /** A list of programming {@linkplain Hook.InitConnection#on hooks} that will be invoked only once when
     * the raw JDBC Connection is first created. Their execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.InitConnection> onInit = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.GetConnection#on hooks} that will be invoked on the raw JDBC
     * Connection as part of the {@link DataSource#getConnection()} flow. Their execution should take as short time as
     * possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.GetConnection> onGet = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.CloseConnection#on hooks} that will be invoked on the raw JDBC
     * Connection as part of the {@link java.sql.Connection#close()} flow. Their execution should take as short time as
     * possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.CloseConnection> onClose = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.DestroyConnection#on hooks} that will be invoked only once when
     * the raw JDBC Connection is closed/destroyed. Their execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.DestroyConnection> onDestroy = EMPTY_LIST;

    public void addOnInit(Hook.InitConnection hook) {
        if (onInit == EMPTY_LIST)
            onInit = new ArrayList<>();
        onInit.add(hook);
    }

    public void addOnGet(Hook.GetConnection hook) {
        if (onGet == EMPTY_LIST)
            onGet = new ArrayList<>();
        onGet.add(hook);
    }

    public void addOnClose(Hook.CloseConnection hook) {
        if (onClose == EMPTY_LIST)
            onClose = new ArrayList<>();
        onClose.add(hook);
    }

    public void addOnDestroy(Hook.DestroyConnection hook) {
        if (onDestroy == EMPTY_LIST)
            onDestroy = new ArrayList<>();
        onDestroy.add(hook);
    }

    List<Hook.InitConnection> onInit() {
        return onInit;
    }

    List<Hook.GetConnection> onGet() {
        return onGet;
    }

    List<Hook.CloseConnection> onClose() {
        return onClose;
    }

    List<Hook.DestroyConnection> onDestroy() {
        return onDestroy;
    }
}
