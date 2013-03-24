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
        this.maxSize = maxSize;
        this.straightMap = new ConcurrentHashMap<K, ValueHolder<V>>(maxSize);
        this.reverseMap = new ConcurrentSkipListMap<Long, K>();
    }

    private static class ValueHolder<V> {
        private final V value;
        private long order;

        private ValueHolder(V value, long order) {
            this.value = value;
            this.order = order;
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ValueHolder that = (ValueHolder) o;
            return order == that.order;
        }

        public int hashCode() {
            return (int) (order ^ (order >>> 32));
        }
    }

    /** {@inheritDoc} */
    public int size() {
        return size.get();
    }

    /** {@inheritDoc} */
    public V get(K key) {
        ValueHolder<V> vh = straightMap.get(key);
        return vh != null ? vh.value : null;
    }

    /** {@inheritDoc} */
    public V putIfAbsent(K key, V value) {
        ValueHolder<V> newVh = new ValueHolder<V>(value, idGenerator.getAndDecrement());
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
                        new ValueHolder<V>(null, lastEntry.getKey()));
                    if (!removed)
                        size.incrementAndGet();
                }
                newSize = size.get();
            }
            return null;
        }
        return oldVh.value;
    }

    /** {@inheritDoc} */
    public V remove(K key) {
        ValueHolder<V> vh = straightMap.remove(key);
        if (vh != null) {
            size.decrementAndGet();
            reverseMap.remove(vh.order);
            return vh.value;
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
