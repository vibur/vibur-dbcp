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

import org.vibur.dbcp.pool.Hook;
import org.vibur.dbcp.pool.StatementProceedingPoint;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.vibur.dbcp.pool.DefaultHook.Util.addHook;

/**
 * Holds all programming method invocation hooks collections.
 *
 * <p>Note that the underlying data structures used to store the Hook instances <b>are not</b> thread-safe
 * for modifications. They must be set only once during the pool configuration phase and must not be modified once
 * the pool is started.
 *
 * @author Simeon Malchev
 */
public class InvocationHooksHolder {

    /** A list of programming {@linkplain Hook.MethodInvocation#on hooks} intercepting (almost) all method calls on all
     * proxied JDBC interfaces. Methods inherited from the {@link Object} class, methods related to the "closed" state
     * of the JDBC objects (e.g., close(), isClosed()), as well as methods from the {@link java.sql.Wrapper} interface
     * are not intercepted. The hooks execution should take as short time as possible. */
    private Hook.MethodInvocation[] onMethodInvocation = new Hook.MethodInvocation[0];

    /** A list of programming {@linkplain Hook.StatementExecution#on hooks} that will be invoked after each JDBC
     * Statement "execute..." method call returns. Their execution should take as short time as possible. */
    private Hook.StatementExecution[] onStatementExecution = new Hook.StatementExecution[0];

    private Hook.StatementExecutionA[] onStatementExecutionA = new Hook.StatementExecutionA[0];

    /** A list of programming {@linkplain Hook.ResultSetRetrieval#on hooks} that will be invoked at the end of each
     * ResultSet retrieval. Their execution should take as short time as possible. */
    private Hook.ResultSetRetrieval[] onResultSetRetrieval = new Hook.ResultSetRetrieval[0];

    public void addOnMethodInvocation(Hook.MethodInvocation hook) {
        onMethodInvocation = addHook(onMethodInvocation, hook);
    }

    public void addOnStatementExecution(Hook.StatementExecution hook) {
        onStatementExecution = addHook(onStatementExecution, hook);
    }

    public void addOnResultSetRetrieval(Hook.ResultSetRetrieval hook) {
        onResultSetRetrieval = addHook(onResultSetRetrieval, hook);
    }

    Hook.MethodInvocation[] onMethodInvocation() {
        return onMethodInvocation;
    }

    Hook.StatementExecution[] onStatementExecution() {
        return onStatementExecution;
    }

    Hook.ResultSetRetrieval[] onResultSetRetrieval() {
        return onResultSetRetrieval;
    }


    public static class Spp implements StatementProceedingPoint {
        private final Hook.StatementExecutionA statementExecutionA;

        public Spp(Hook.StatementExecutionA statementExecutionA) {
            this.statementExecutionA = statementExecutionA;
        }

        @Override
        public Statement proxy() {
            return null;
        }

        @Override
        public Method method() {
            return null;
        }

        @Override
        public Object[] args() {
            return new Object[0];
        }

        @Override
        public Object proceed() throws SQLException {
            return null;
        }

        @Override
        public List<Object[]> queryParams() {
            return null;
        }

        @Override
        public String sqlQuery() {
            return null;
        }

        @Override
        public Object proceed(Object[] args) throws SQLException {
            return null;
        }
    }
}
