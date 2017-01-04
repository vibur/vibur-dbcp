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

import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.cache.ClhmStatementCache;
import org.vibur.dbcp.cache.StatementHolder;
import org.vibur.dbcp.cache.StatementMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

/**
 * @author Simeon Malchev
 */
public class StatementCacheUtils {

    @SuppressWarnings("unchecked")
    public static ConcurrentMap<StatementMethod, StatementHolder> mockStatementCache(ViburDBCPDataSource ds) {
        final List<ConcurrentMap<StatementMethod, StatementHolder>> holder = new ArrayList<>();

        ds.setStatementCache(new ClhmStatementCache(ds.getStatementCacheMaxSize()) {
            @Override
            protected ConcurrentMap<StatementMethod, StatementHolder> buildStatementCache(int maxSize) {
                ConcurrentMap<StatementMethod, StatementHolder> mockedStatementCache =
                        mock(ConcurrentMap.class, delegatesTo(super.buildStatementCache(maxSize)));
                holder.add(mockedStatementCache);
                return mockedStatementCache;
            }
        });

        return holder.get(0);
    }
}
