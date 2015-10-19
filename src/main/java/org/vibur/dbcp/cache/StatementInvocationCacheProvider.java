/**
 * Copyright 2014 Simeon Malchev
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

import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.slf4j.LoggerFactory;

import java.sql.Statement;

import static org.vibur.dbcp.cache.StatementVal.AVAILABLE;
import static org.vibur.dbcp.cache.StatementVal.EVICTED;
import static org.vibur.dbcp.util.SqlUtils.closeStatement;

/**
 * A concurrent cache provider for JDBC Statement method invocations which maps
 * {@code ConnMethodKey} to {@code StatementVal}, based on ConcurrentLinkedHashMap.
 *
 * @author Simeon Malchev
 */
public class StatementInvocationCacheProvider extends AbstractInvocationCacheProvider {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(StatementInvocationCacheProvider.class);

    public StatementInvocationCacheProvider(int maxSize) {
        super(maxSize);
    }

    protected EvictionListener<ConnMethodKey, StatementVal> getListener() {
        return new EvictionListener<ConnMethodKey, StatementVal>() {
            public void onEviction(ConnMethodKey key, StatementVal value) {
                if (value.state().getAndSet(EVICTED) == AVAILABLE)
                    closeStatement(value.value());
                logger.trace("Evicted {}", value.value());
            }
        };
    }
}
