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

import static java.lang.String.format;

/**
 * @author Simeon Malchev
 */
public final class ViburUtils {

    private ViburUtils() {}

    public static String getPoolName(ViburConfig config) {
        BasePool pool = config.getPool();
        return format("%s@%h (%d/%d)", config.getName(), config.hashCode(), pool.taken(), pool.remainingCreated());
    }

    public static String getStackTraceAsString(StackTraceElement[] stackTrace) {
        int i;
        for (i = 0; i < stackTrace.length; i++) {
            if (!stackTrace[i].getClassName().startsWith("org.vibur")
                || stackTrace[i].getMethodName().equals("getConnection"))
                break;
        }

        StringBuilder builder = new StringBuilder(4096);
        for (i++ ; i < stackTrace.length; i++)
            builder.append("  at ").append(stackTrace[i]).append('\n');
        return builder.toString();
    }

    public static String formatSql(String sqlQuery, List<Object[]> queryParams) {
        StringBuilder result = new StringBuilder(1024).append("-- ").append(sqlQuery);
        if (queryParams != null && !queryParams.isEmpty())
            result.append("\n-- Parameters:\n-- ").append(Arrays.deepToString(queryParams.toArray()));
        return result.toString();
    }
}
