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

package org.vibur.dbcp.util;

import org.vibur.dbcp.ViburConfig;
import org.vibur.objectpool.BasePool;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.Integer.toHexString;
import static java.util.Comparator.comparing;

/**
 * @author Simeon Malchev
 */
public final class ViburUtils {

    private ViburUtils() { }

    /**
     * Returns the extended pool name formatted as:
     * <blockquote>{@code poolName@hashCode(currentlyTakenConns/remainingCreatedConns/poolMaxSize/poolState/threadInterruptedStatus)}</blockquote>
     * For example, {@code p1@2db7a79b(1/1/10/w/n)}.
     *
     * @param config the Vibur config
     */
    public static String getPoolName(ViburConfig config) {
        BasePool pool = config.getPool();
        var initialState = pool.isTerminated();
        var result = config.getName() + '@' + toHexString(config.hashCode())
                + '(' + pool.taken() + '/' + pool.remainingCreated() + '/' + pool.maxSize()
                + '/' + (!initialState ? 'w' : 't')  // poolState: w == working, t == terminated
                + '/' + (Thread.currentThread().isInterrupted() ? 'i' : 'n') + ')';
        if (initialState == pool.isTerminated()) { // make sure the pool state has not changed in the meantime
            return result;
        }
        return getPoolName(config); // this is one level of recursion only, pool state changes only once
    }

    public static String getStackTraceAsString(Pattern logLinePattern, StackTraceElement[] stackTrace) {
        if (stackTrace == null || stackTrace.length == 0) {
            return "EMPTY STACK TRACE\n";
        }

        int i;
        for (i = 0; i < stackTrace.length; i++) {
            if (!stackTrace[i].getClassName().startsWith("org.vibur")
                || stackTrace[i].getMethodName().equals("getConnection")) {
                break;
            }
        }

        var builder = new StringBuilder(4096);
        for (i++; i < stackTrace.length; i++) {
            var stackTraceStr = stackTrace[i].toString();
            if (logLinePattern == null || logLinePattern.matcher(stackTraceStr).matches()) {
                builder.append("  at ").append(stackTraceStr).append('\n');
            }
        }
        return builder.toString();
    }

    public static String formatSql(String sqlQuery, List<Object[]> sqlQueryParams) {
        var result = new StringBuilder(1024).append("-- ").append(sqlQuery);

        if (sqlQueryParams != null && !sqlQueryParams.isEmpty()) {
            var params = sqlQueryParams.toArray();
            Arrays.sort(params, comparing(obj -> ((Object[]) obj)[1].toString()));

            result.append("\n-- Parameters:\n-- ").append(Arrays.deepToString(params));
        }
        return result.toString();
    }
}
