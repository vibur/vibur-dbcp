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
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

/**
 * Holds all programming hooks collections.
 *
 * <p>Note that the underlying data structures used to store the Hook instances <b>are not</b> thread-safe
 * for modifications.
 *
 * @author Simeon Malchev
 */
public class HooksHolder {

    /** A list of programming {@linkplain Hook.InitConnection#on hooks} that will be invoked only once when
     * the raw JDBC Connection is first created. Their execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.InitConnection> initConnection = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.GetConnection#on hooks} that will be invoked on the raw JDBC
     * Connection as part of the {@link DataSource#getConnection()} flow. Their execution should take as short time as
     * possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.GetConnection> getConnection = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.CloseConnection#on hooks} that will be invoked on the raw JDBC
     * Connection as part of the {@link java.sql.Connection#close()} flow. Their execution should take as short time as
     * possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.CloseConnection> closeConnection = EMPTY_LIST;

    /** A list of programming {@linkplain Hook.DestroyConnection#on hooks} that will be invoked only once when
     * the raw JDBC Connection is closed/destroyed. Their execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.DestroyConnection> destroyConnection = EMPTY_LIST;


    /** A list of programming {@linkplain Hook.MethodInvocation#on hooks} intercepting (almost) all method calls on all
     * proxied JDBC interfaces. Methods inherited from the {@link Object} class, methods related to the "closed" state
     * of the JDBC objects (e.g., close(), isClosed()), as well as methods from the {@link java.sql.Wrapper} interface
     * are not intercepted. The hooks execution should take as short time as possible. */
    @SuppressWarnings("unchecked")
    private List<Hook.MethodInvocation> methodInvocation = EMPTY_LIST;


    public void addInitConnection(Hook.InitConnection hook) {
        if (initConnection == EMPTY_LIST)
            initConnection = new ArrayList<>();
        initConnection.add(hook);
    }

    public void addGetConnection(Hook.GetConnection hook) {
        if (getConnection == EMPTY_LIST)
            getConnection = new ArrayList<>();
        getConnection.add(hook);
    }

    public void addCloseConnection(Hook.CloseConnection hook) {
        if (closeConnection == EMPTY_LIST)
            closeConnection = new ArrayList<>();
        closeConnection.add(hook);
    }

    public void addDestroyConnection(Hook.DestroyConnection hook) {
        if (destroyConnection == EMPTY_LIST)
            destroyConnection = new ArrayList<>();
        destroyConnection.add(hook);
    }

    public void addMethodInvocation(Hook.MethodInvocation hook) {
        if (methodInvocation == EMPTY_LIST)
            methodInvocation = new ArrayList<>();
        methodInvocation.add(hook);
    }

    List<Hook.InitConnection> initConnection() {
        return initConnection;
    }

    List<Hook.GetConnection> getConnection() {
        return getConnection;
    }

    List<Hook.CloseConnection> closeConnection() {
        return closeConnection;
    }

    List<Hook.DestroyConnection> destroyConnection() {
        return destroyConnection;
    }

    public List<Hook.MethodInvocation> methodInvocation() {
        return methodInvocation;
    }
}
