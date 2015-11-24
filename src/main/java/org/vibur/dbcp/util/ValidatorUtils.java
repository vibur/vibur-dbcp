/**
 * Copyright 2015 Simeon Malchev
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

import org.vibur.dbcp.restriction.ConnectionRestriction;

/**
 * SQL query restriction validation utils.
 *
 * @author Simeon Malchev
 */
public class ValidatorUtils {

    private ValidatorUtils() {}

    public static boolean isQueryAllowed(String sql, ConnectionRestriction restriction) {
        if (restriction == null || restriction.restrictedQueryPrefixes() == null)
            return true;

        String firstWord = getFirstWord(sql);
        if (firstWord == null)
            return true;

        return restriction.restrictedQueryPrefixes().contains(firstWord) == restriction.whiteListed();
    }

    private static String getFirstWord(String sql) {
        if (sql == null || sql.isEmpty())
            return null;

        int len = sql.length();
        char[] sqlChars = new char[len];
        sql.getChars(0, len, sqlChars, 0);

        int beg = 0;
        for (int i = 0; i < len && Character.isWhitespace((int) sqlChars[i]); i++)
            beg++;
        if (beg == len)
            return null;

        int end = beg;
        for (int i = beg; i < len && !Character.isWhitespace((int) sqlChars[i]); i++) {
            sqlChars[i] = (char) Character.toLowerCase((int) sqlChars[i]);
            end++;
        }
        return new String(sqlChars, beg, end - beg);
    }
}
