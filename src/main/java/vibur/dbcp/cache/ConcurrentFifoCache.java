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

package vibur.dbcp.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent cache interface, a subset of {@link java.util.concurrent.ConcurrentNavigableMap}.
 *
 * @author Simeon Malchev
 */
public class ConcurrentFifoCache<K, V> implements ConcurrentCache<K, V> {

    private final ConcurrentMap<K, ValueHolder<V>> straightMap;
    private final NavigableMap<Long, K> reverseMap;

    private final AtomicInteger size = new AtomicInteger(0);
    private final AtomicLong idGenerator = new AtomicLong(Long.MAX_VALUE);

    private final int maxSize;

    public ConcurrentFifoCache(int maxSize) {
        if (maxSize < 1)
            throw new IllegalArgumentException();
        this.maxSize = maxSize;
        this.straightMap = new ConcurrentHashMap<K, ValueHolder<V>>(maxSize);
        this.reverseMap = new ConcurrentSkipListMap<Long, K>();
    }

    /** {@inheritDoc} */
    public int size() {
        return size.get();
    }

    /** {@inheritDoc} */
    public V get(K key) {
        ValueHolder<V> vh = straightMap.get(key);
        return vh != null ? vh.getValue() : null;
    }

    /** {@inheritDoc} */
    public ValueHolder<V> take(K key) {
        return straightMap.get(key);
    }

    /** {@inheritDoc} */
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, true);
    }

    /** {@inheritDoc} */
    public V putIfAbsent(K key, V value, boolean available) {
        ValueHolder<V> newVh = new ValueHolder<V>(idGenerator.getAndDecrement(),
            value, new AtomicBoolean(available));
        ValueHolder<V> oldVh = straightMap.putIfAbsent(key, newVh);
        if (oldVh == null) {
            reverseMap.put(newVh.order, key);

            int newSize = size.incrementAndGet();
            while (newSize > maxSize) {
                if (size.compareAndSet(newSize, newSize - 1)) {
                    Map.Entry<Long, K> lastEntry = reverseMap.pollLastEntry();
                    if (lastEntry == null)
                        break;
                    boolean removed = straightMap.remove(lastEntry.getValue(),
                        new ValueHolder<V>(lastEntry.getKey(), null, null));
                    if (!removed)
                        size.incrementAndGet();
                }
                newSize = size.get();
            }
            return null;
        }
        return oldVh.getValue();
    }

    /** {@inheritDoc} */
    public V remove(K key) {
        ValueHolder<V> vh = straightMap.remove(key);
        if (vh != null) {
            size.decrementAndGet();
            reverseMap.remove(vh.order);
            return vh.getValue();
        }
        return null;
    }

    /** {@inheritDoc} */
    public void clear() {
        for (Iterator<Map.Entry<K, ValueHolder<V>>> i =
                 straightMap.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<K, ValueHolder<V>> entry = i.next();
            i.remove();
            reverseMap.remove(entry.getValue().order);
            size.decrementAndGet();
        }
    }

    /** {@inheritDoc} */
    public K firstKey() {
        Map.Entry<Long, K> entry = reverseMap.firstEntry();
        return entry != null ? entry.getValue() : null;
    }

    /** {@inheritDoc} */
    public K lastKey() {
        Map.Entry<Long, K> entry = reverseMap.lastEntry();
        return entry != null ? entry.getValue() : null;
    }

    /** {@inheritDoc} */
    public Set<K> keySet() {
        Set<K> sorted = new TreeSet<K>(Collections.reverseOrder());
        sorted.addAll(reverseMap.values());
        return sorted;
    }
}
