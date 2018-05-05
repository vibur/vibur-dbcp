/**
 * Copyright 2013 Simeon Malchev
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

package org.vibur.dbcp.stcache;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import static java.lang.String.format;

/**
 * Describes a {@code prepareStatement} or {@code prepareCall} method with {@code args} that has been invoked on a
 * given JDBC Connection.
 *
 * <p>Used as a caching {@code key} for the above mentioned Connection method invocations in a {@code ConcurrentMap}
 * cache implementation.
 *
 * @see StatementHolder
 *
 * @author Simeon Malchev
 */
public class StatementMethod {

    public interface StatementCreator { // for internal use only
        PreparedStatement newStatement(Method method, Object[] args) throws SQLException;
    }

    private final StatementCreator statementCreator;
    private final Connection rawConnection; // the underlying raw JDBC Connection
    private final Method method; // the invoked prepareStatement(...) or prepareCall(...) method
    private final Object[] args; // the invoked method args

    public StatementMethod(Connection rawConnection, StatementCreator statementCreator, Method method, Object[] args) {
        assert statementCreator != null;
        assert method != null;
        assert args != null && args.length >= 1;
        this.statementCreator = statementCreator;
        this.rawConnection = rawConnection;
        this.method = method;
        this.args = args;
    }

    Connection rawConnection() {
        return rawConnection;
    }

    PreparedStatement newStatement() throws SQLException {
        return statementCreator.newStatement(method, args);
    }

    String sqlQuery() {
        return (String) args[0]; // as only prepared and callable Statements are cached the args[0] is the query
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StatementMethod that = (StatementMethod) o;
        return rawConnection == that.rawConnection // comparing with == as the JDBC Connections are pooled objects
            && method.equals(that.method)
            && Arrays.equals(args, that.args);
    }

    @Override
    public int hashCode() {
        int result = rawConnection.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + Arrays.hashCode(args);
        return result;
    }

    @Override
    public String toString() {
        return format("rawConnection %s, method %s, args %s", rawConnection, method, Arrays.toString(args));
    }
}
