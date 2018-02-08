/**
 * Copyright 2017 Simeon Malchev
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
 * This class holds and isolates all application programming hook collection interfaces and their private
 * implementation classes.
 */
public final class HookHolder {

    private HookHolder() { }

    public static ConnHooks newConnHooks() {
        return new ConnHooksHolder();
    }

    public static InvocationHooks newInvocationHooks() {
        return new InvocationHooksHolder();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // The hooks collections interfaces:

    public interface ConnHooks {
        void addOnInit(Hook.InitConnection hook);
        void addOnGet(Hook.GetConnection hook);
        void addOnClose(Hook.CloseConnection hook);
        void addOnDestroy(Hook.DestroyConnection hook);
        void addOnTimeout(Hook.GetConnectionTimeout hook);
    }

    interface ConnHooksAccessor { // for internal use only
        Hook.InitConnection[] onInit();
        Hook.GetConnection[] onGet();
        Hook.CloseConnection[] onClose();
        Hook.DestroyConnection[] onDestroy();
        Hook.GetConnectionTimeout[] onTimeout();
    }

    public interface InvocationHooks {
        void addOnMethodInvocation(Hook.MethodInvocation hook);
        void addOnStatementExecution(Hook.StatementExecution hook);
        void addOnResultSetRetrieval(Hook.ResultSetRetrieval hook);
    }

    public interface InvocationHooksAccessor { // for internal use only
        Hook.MethodInvocation[] onMethodInvocation();
        Hook.StatementExecution[] onStatementExecution();
        Hook.ResultSetRetrieval[] onResultSetRetrieval();
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // The hooks collections implementing classes:

    private static class ConnHooksHolder implements ConnHooks, ConnHooksAccessor {

        /** A list of programming {@linkplain Hook.InitConnection#on hooks} that will be invoked only once <i>after</i>
         * the raw JDBC Connection is first created. Their execution should take as short time as possible. */
        private Hook.InitConnection[] onInit = {};

        /** A list of programming {@linkplain Hook.GetConnection#on hooks} that will be invoked on the raw JDBC Connection
         * <i>after</i> it was taken from the pool as part of the {@link DataSource#getConnection()} flow.
         * Their execution should take as short time as possible. */
        private Hook.GetConnection[] onGet = {};

        /** A list of programming {@linkplain Hook.CloseConnection#on hooks} that will be invoked on the raw JDBC Connection
         * <i>before</i> it is restored back to the pool as part of the {@link java.sql.Connection#close()} flow.
         * Their execution should take as short time as possible. */
        private Hook.CloseConnection[] onClose = {};

        /** A list of programming {@linkplain Hook.DestroyConnection#on hooks} that will be invoked only once <i>after</i>
         * the raw JDBC Connection is closed/destroyed. Their execution should take as short time as possible. */
        private Hook.DestroyConnection[] onDestroy = {};

        /** A list of programming {@linkplain Hook.DestroyConnection#on hooks} that will be invoked only if the call
         * to {@link org.vibur.dbcp.ViburDataSource#getConnection()} timeouts. Their execution should take as short time
         * as possible. */
        private Hook.GetConnectionTimeout[] onTimeout = {};

        @Override
        public void addOnInit(Hook.InitConnection hook) {
            onInit = addHook(onInit, hook);
        }

        @Override
        public void addOnGet(Hook.GetConnection hook) {
            onGet = addHook(onGet, hook);
        }

        @Override
        public void addOnClose(Hook.CloseConnection hook) {
            onClose = addHook(onClose, hook);
        }

        @Override
        public void addOnDestroy(Hook.DestroyConnection hook) {
            onDestroy = addHook(onDestroy, hook);
        }

        @Override
        public void addOnTimeout(Hook.GetConnectionTimeout hook) {
            onTimeout = addHook(onTimeout, hook);
        }

        @Override
        public Hook.InitConnection[] onInit() {
            return onInit;
        }

        @Override
        public Hook.GetConnection[] onGet() {
            return onGet;
        }

        @Override
        public Hook.CloseConnection[] onClose() {
            return onClose;
        }

        @Override
        public Hook.DestroyConnection[] onDestroy() {
            return onDestroy;
        }

        @Override
        public Hook.GetConnectionTimeout[] onTimeout() {
            return onTimeout;
        }
    }

    private static class InvocationHooksHolder implements InvocationHooks, InvocationHooksAccessor {

        /** A list of programming {@linkplain Hook.MethodInvocation#on hooks} that will be invoked <i>before</i> (almost) all
         * methods on the proxied JDBC interfaces. Methods inherited from the {@link Object} class, methods related to the
         * "closed" state of the JDBC objects (e.g., close(), isClosed()), as well as methods from the {@link java.sql.Wrapper}
         * interface are not intercepted. The hooks execution should take as short time as possible. */
        private Hook.MethodInvocation[] onMethodInvocation = {};

        /** A list of programming {@linkplain Hook.StatementExecution#on hooks} that will be invoked <i>around</i> the call
         * of each JDBC Statement "execute..." method. Their execution should take as short time as possible. */
        private Hook.StatementExecution[] onStatementExecution = {};

        /** A list of programming {@linkplain Hook.ResultSetRetrieval#on hooks} that will be invoked <i>at the end</i> of
         * each ResultSet retrieval. Their execution should take as short time as possible. */
        private Hook.ResultSetRetrieval[] onResultSetRetrieval = {};

        @Override
        public void addOnMethodInvocation(Hook.MethodInvocation hook) {
            onMethodInvocation = addHook(onMethodInvocation, hook);
        }

        @Override
        public void addOnStatementExecution(Hook.StatementExecution hook) {
            onStatementExecution = addHook(onStatementExecution, hook);
        }

        @Override
        public void addOnResultSetRetrieval(Hook.ResultSetRetrieval hook) {
            onResultSetRetrieval = addHook(onResultSetRetrieval, hook);
        }

        @Override
        public Hook.MethodInvocation[] onMethodInvocation() {
            return onMethodInvocation;
        }

        @Override
        public Hook.StatementExecution[] onStatementExecution() {
            return onStatementExecution;
        }

        @Override
        public Hook.ResultSetRetrieval[] onResultSetRetrieval() {
            return onResultSetRetrieval;
        }
    }
}
