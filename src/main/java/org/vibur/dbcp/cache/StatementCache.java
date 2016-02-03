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

package org.vibur.dbcp.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.proxy.TargetInvoker;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.vibur.dbcp.cache.StatementVal.*;
import static org.vibur.dbcp.util.JdbcUtils.clearWarnings;
import static org.vibur.dbcp.util.JdbcUtils.closeStatement;

/**
 * Implements and encapsulates all JDBC Statement caching functionality and logic.
 *
 * @author Simeon Malchev
 */
public class StatementCache {

    private static final Logger logger = LoggerFactory.getLogger(StatementCache.class);

    private final ConcurrentMap<ConnMethodKey, StatementVal> statementCache;

    public StatementCache(int maxSize) {
        if (maxSize <= 0)
            throw new IllegalArgumentException();
        statementCache = buildStatementCache(maxSize);
    }

    protected ConcurrentMap<ConnMethodKey, StatementVal> buildStatementCache(int maxSize) {
        return new ConcurrentLinkedHashMap.Builder<ConnMethodKey, StatementVal>()
                .initialCapacity(maxSize)
                .maximumWeightedCapacity(maxSize)
                .listener(getListener())
                .build();
    }

    /**
     * Creates and returns a new EvictionListener for the CLHM. It is worth noting that this
     * EvictionListener is called in the context of the thread that has executed an insert (putIfAbsent)
     * operation which has increased the CLHM size above its maxSize - in which case the CLHM
     * evicts its LRU entry.
     *
     * @return a new EvictionListener for the CLHM
     */
    protected EvictionListener<ConnMethodKey, StatementVal> getListener() {
        return new EvictionListener<ConnMethodKey, StatementVal>() {
            @Override
            public void onEviction(ConnMethodKey key, StatementVal value) {
                if (value.state().getAndSet(EVICTED) == AVAILABLE)
                    closeStatement(value.value());
                logger.trace("Evicted {}", value.value());
            }
        };
    }

    /**
     * Returns <i>a possibly</i> cached StatementVal object for the given connection method key.
     *
     * @param key the connection method key
     * @param invoker the invoker through which to create the raw JDBC Statement object, if needed
     * @return a retrieved from the cache or newly created StatementVal holder object wrapping the raw JDBC Statement object
     * @throws Throwable if the invoked underlying prepareXYZ method throws an exception
     */
    public StatementVal retrieve(ConnMethodKey key, TargetInvoker invoker) throws Throwable {
        StatementVal statement = statementCache.get(key);
        if (statement == null || !statement.state().compareAndSet(AVAILABLE, IN_USE)) {
            Statement rawStatement = (Statement) invoker.targetInvoke(key.getMethod(), key.getArgs());
            if (statement == null) { // there was no entry for the key, so we'll try to put a new one
                statement = new StatementVal(rawStatement, new AtomicInteger(IN_USE));
                if (statementCache.putIfAbsent(key, statement) == null)
                    return statement; // the new entry was successfully put in the cache
            }
            return new StatementVal(rawStatement, null);
        } else { // the statement was in the cache and was available
            logger.trace("Using cached statement for {}", key);
            return statement;
        }
    }

    public void restore(StatementVal statement, boolean clearWarnings) {
        if (statement.state() == null) // this statement is not in the cache
            return;

        Statement rawStatement = statement.value();
        if (clearWarnings)
            clearWarnings(rawStatement);
        if (!statement.state().compareAndSet(IN_USE, AVAILABLE)) // just mark it as available if its state was in_use
            closeStatement(rawStatement); // and close it if it was already evicted (while its state was in_use)
    }

    public boolean remove(Statement rawStatement, boolean close) {
        for (Map.Entry<ConnMethodKey, StatementVal> entry : statementCache.entrySet()) {
            StatementVal value = entry.getValue();
            if (value.value() == rawStatement) { // comparing with == as these JDBC Statements are cached objects
                if (close)
                    closeStatement(rawStatement);
                return statementCache.remove(entry.getKey(), value);
            }
        }
        return false;
    }

    public int removeAll(Connection rawConnection) {
        int removed = 0;
        for (Map.Entry<ConnMethodKey, StatementVal> entry : statementCache.entrySet()) {
            ConnMethodKey key = entry.getKey();
            StatementVal value = entry.getValue();
            if (key.getTarget() == rawConnection && statementCache.remove(key, value)) {
                closeStatement(value.value());
                removed++;
            }
        }
        return removed;
    }

    public void clear() {
        for (Map.Entry<ConnMethodKey, StatementVal> entry : statementCache.entrySet()) {
            StatementVal value = entry.getValue();
            statementCache.remove(entry.getKey(), value);
            closeStatement(value.value());
        }
    }
}
