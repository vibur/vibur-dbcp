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

import java.sql.Connection;
import java.sql.Statement;

import static org.vibur.dbcp.util.SqlUtils.closeStatement;

/**
 * A concurrent cache provider for JDBC Statement method invocations which maps
 * {@code MethodDef<Connection>} to {@code ReturnVal<Statement>}, based on ConcurrentLinkedHashMap.
 *
 * @author Simeon Malchev
 */
public class StatementInvocationCacheProvider extends AbstractInvocationCacheProvider<Connection, Statement> {

    public StatementInvocationCacheProvider(int maxSize) {
        super(maxSize);
    }

    protected EvictionListener<MethodDef<Connection>, ReturnVal<Statement>> getListener() {
        return new EvictionListener<MethodDef<Connection>, ReturnVal<Statement>>() {
            public void onEviction(MethodDef<Connection> key, ReturnVal<Statement> value) {
                closeStatement(value.value());
            }
        };
    }
}
