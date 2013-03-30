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

package vibur.dbcp.util;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;

/**
 * @author Simeon Malchev
 */
public class StatementUtils {

    private StatementUtils() {}

    public static String toSQLString(Statement statementProxy, Object[] args) {
        if (statementProxy instanceof PreparedStatement)
            return statementProxy.toString();

        return Arrays.toString(args); // when a simple JDBC Statement
    }
}
