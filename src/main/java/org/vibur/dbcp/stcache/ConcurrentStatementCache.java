/**
 * Copyright 2015-2025 Simeon Malchev
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.vibur.dbcp.stcache.StatementHolder.State.AVAILABLE;
import static org.vibur.dbcp.stcache.StatementHolder.State.EVICTED;
import static org.vibur.dbcp.stcache.StatementHolder.State.IN_USE;
import static org.vibur.dbcp.util.JdbcUtils.clearWarnings;
import static org.vibur.dbcp.util.JdbcUtils.quietClose;
import static org.vibur.objectpool.util.ArgumentValidation.forbidIllegalArgument;

/**
 * Implements and encapsulates all JDBC Statement caching functionality and logic. The cache implementation is
 * based on {@link Caffeine}.
 *
 * @author Simeon Malchev
 */
public class ConcurrentStatementCache implements StatementCache {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentStatementCache.class);

    private final ConcurrentMap<StatementMethod, StatementHolder> statementCache;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConcurrentStatementCache(int maxSize) {
        forbidIllegalArgument(maxSize <= 0, "cache.maxSize");
        statementCache = requireNonNull(buildStatementCache(maxSize));
    }

    ConcurrentMap<StatementMethod, StatementHolder> buildStatementCache(int maxSize) {
        return Caffeine.newBuilder()
                .initialCapacity(maxSize)
                .maximumSize(maxSize)
                .evictionListener(getEvictionListener())
                .build()
                .asMap();
    }

    /**
     * Creates and returns a new EvictionListener for Caffeine. It is worth noting that this
     * EvictionListener is called in the context of the thread that has executed an insert (putIfAbsent)
     * operation which has increased the Caffeine size above its maxSize - in which case Caffeine
     * evicts its LRU entry.
     *
     * @return a new EvictionListener for Caffeine
     */
    private static RemovalListener<StatementMethod, StatementHolder> getEvictionListener() {
        return (statementMethod, statementHolder, cause) -> {
            if (statementHolder == null || !cause.wasEvicted()) {
                return;
            }
            if (statementHolder.state().getAndSet(EVICTED) == AVAILABLE) {
                quietClose(statementHolder.rawStatement());
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Evicted {}", statementHolder.rawStatement());
            }
        };
    }

    @Override
    public StatementHolder take(StatementMethod statementMethod) throws SQLException {
        if (isClosed()) {
            return new StatementHolder(statementMethod.newStatement(), null, statementMethod.sqlQuery());
        }

        var statement = statementCache.get(statementMethod);
        if (statement != null) {
            if (statement.state().compareAndSet(AVAILABLE, IN_USE)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Using cached statement for {}", statementMethod);
                }
                return statement;
            }
            // if the statement in the cache was not available we return an uncached StatementHolder
            return new StatementHolder(statementMethod.newStatement(), null, statementMethod.sqlQuery());
        }

        // there was no cache entry for the statementMethod, so we'll try to put a new one
        var rawStatement = statementMethod.newStatement();
        statement = new StatementHolder(rawStatement, new AtomicReference<>(IN_USE), statementMethod.sqlQuery());
        if (statementCache.putIfAbsent(statementMethod, statement) == null) {
            return statement; // the new entry was successfully put in the cache, so we return it
        }
        // if we couldn't put the statement in the cache we return an uncached StatementHolder
        return new StatementHolder(rawStatement, null, statementMethod.sqlQuery());
    }

    @Override
    public boolean restore(StatementHolder statement, boolean clearWarnings) {
        if (isClosed()) {
            remove(statement);
            return false;
        }
        if (statement.state() == null) { // this statement is not in the cache
            return false;
        }

        var rawStatement = (PreparedStatement) statement.rawStatement();
        try {
            if (clearWarnings) {
                clearWarnings(rawStatement);
            }
            return statement.state().compareAndSet(IN_USE, AVAILABLE); // we just mark it as AVAILABLE if it was IN_USE
        } catch (SQLException e) {
            logger.debug("Couldn't clear warnings on {}", rawStatement, e);
            remove(statement);
            return false;
        }
    }

    @Override
    public boolean remove(StatementHolder statement) {
        if (statement.state() == null) { // this statement is not in the cache
            return false;
        }

        for (var entry : statementCache.entrySet()) {
            var value = entry.getValue();
            if (value == statement) { // comparing with == as these JDBC Statements are cached objects
                return statementCache.remove(entry.getKey(), value);
            }
        }
        return false;
    }

    @Override
    public int removeAll(Connection rawConnection) {
        var removed = 0;
        for (var entry : statementCache.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            if (key.rawConnection() == rawConnection && statementCache.remove(key, value)) {
                quietClose(value.rawStatement());
                removed++;
            }
        }
        return removed;
    }

    /**
     * Closes this ConcurrentStatementCache and removes all entries from it.
     */
    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }

        for (var entry : statementCache.entrySet()) {
            var value = entry.getValue();
            statementCache.remove(entry.getKey(), value);
            quietClose(value.rawStatement());
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
