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

import org.vibur.dbcp.restriction.QueryRestriction;

/**
 * SQL query restriction validation utils.
 *
 * @author Simeon Malchev
 */
public class QueryValidatorUtils {

    private QueryValidatorUtils() {}

    /**
     * Returns true if the given SQL query is allowed by the given query restrictions, false otherwise.
     *
     * @param sql the SQL query to be checked
     * @param restriction the restrictions against which the query will be checked
     * @return see above
     */
    public static boolean isQueryAllowed(String sql, QueryRestriction restriction) {
        if (restriction == null || restriction.restrictedPrefixes() == null)
            return true;

        String[] words = getFirstTwoWords(sql);
        if (words == null)
            return true;

        if (restriction.whiteListed())
            return restriction.restrictedPrefixes().contains(words[0]) == restriction.whiteListed()
                    || restriction.restrictedPrefixes().contains(words[1]) == restriction.whiteListed();
        else
            return restriction.restrictedPrefixes().contains(words[0]) == restriction.whiteListed()
                    && restriction.restrictedPrefixes().contains(words[1]) == restriction.whiteListed();
    }

    private static String[] getFirstTwoWords(String sql) {
        if (sql == null || sql.isEmpty())
            return null;

        int len = sql.length();
        char[] sqlChars = new char[len];
        sql.getChars(0, len, sqlChars, 0);

        int firstBeg = skipWhitespaces(sqlChars, 0);
        if (firstBeg == len)
            return null;

        int firstEnd = lowerCaseWordEnd(sqlChars, firstBeg, 0);
        String firstWord = new String(sqlChars, firstBeg, firstEnd - firstBeg);

        int secondBeg = skipWhitespaces(sqlChars, firstEnd);
        if (secondBeg == len)
            return new String[] {firstWord, null};

        int secondEnd = lowerCaseWordEnd(sqlChars, secondBeg, secondBeg - firstEnd - 1);
        String firstTwoWords = new String(sqlChars, firstBeg, secondEnd - firstBeg);
        return new String[] {firstWord, firstTwoWords};
    }

    private static int skipWhitespaces(char[] sqlChars, int from) {
        int beg = from;
        while (beg < sqlChars.length && Character.isWhitespace((int) sqlChars[beg]))
            beg++;
        return beg;
    }

    private static int lowerCaseWordEnd(char[] sqlChars, int from, int skip) {
        int end = from;
        for (; end < sqlChars.length && !Character.isWhitespace((int) sqlChars[end]); end++)
            sqlChars[end - skip] = (char) Character.toLowerCase((int) sqlChars[end]);
        return end - skip;
    }
}
