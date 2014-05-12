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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import java.util.concurrent.ConcurrentMap;

/**
 * An abstract concurrent cache provider mapping {@code MethodDef<T>} to {@code ReturnVal<V>},
 * based on ConcurrentLinkedHashMap.
 *
 * @author Simeon Malchev
 */
public abstract class AbstractCacheProvider<T, V> {

    private final int maxSize;

    /**
     * Instantiate this abstract concurrent cache provider.
     *
     * @param maxSize the maximum number of elements in the cache
     */
    public AbstractCacheProvider(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException();
        this.maxSize = maxSize;
    }

    /**
     * Builds the concurrent cache map.
     *
     * @return the concurrent cache map
     */
    public ConcurrentMap<MethodDef<T>, ReturnVal<V>> build() {
        return new ConcurrentLinkedHashMap.Builder<MethodDef<T>, ReturnVal<V>>()
                .initialCapacity(maxSize)
                .maximumWeightedCapacity(maxSize)
                .listener(getListener())
                .build();
    }

    protected abstract EvictionListener<MethodDef<T>, ReturnVal<V>> getListener();
}
