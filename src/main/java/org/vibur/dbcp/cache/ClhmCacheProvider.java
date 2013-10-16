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

package org.vibur.dbcp.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import java.sql.Statement;
import java.util.concurrent.ConcurrentMap;

import static org.vibur.dbcp.util.StatementUtils.closeStatement;

/**
 * ConcurrentLinkedHashMap cache provider. Currently, this is the only cache provider implemented.
 *
 * @author Simeon Malchev
 */
public class ClhmCacheProvider {

    private ClhmCacheProvider() { }

    public static ConcurrentMap<StatementKey, ValueHolder<Statement>> buildStatementCache(int statementCacheMaxSize) {
        EvictionListener<StatementKey, ValueHolder<Statement>> listener =
            new EvictionListener<StatementKey, ValueHolder<Statement>>() {
                public void onEviction(StatementKey key, ValueHolder<Statement> value) {
                    closeStatement(value.value());
                }
            };
        return new ConcurrentLinkedHashMap.Builder<StatementKey, ValueHolder<Statement>>()
            .initialCapacity(statementCacheMaxSize)
            .maximumWeightedCapacity(statementCacheMaxSize)
            .listener(listener)
            .build();
    }
}
