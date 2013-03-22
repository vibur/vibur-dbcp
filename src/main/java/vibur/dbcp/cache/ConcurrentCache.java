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

import java.util.Set;

/**
 * Concurrent cache interface, a subset of {@link java.util.concurrent.ConcurrentNavigableMap}.
 *
 * @author Simeon Malchev
 */
public interface ConcurrentCache<K, V> {

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    int size();

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or
     *         {@code null} if this map contains no mapping for the key
     */
    V get(K key);

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old value
     * is replaced by the specified value.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    V put(K key, V value);

    /**
     * Removes the mapping for a key from this map if it is present.
     *
     * <p>Returns the value to which this map previously associated the key,
     * or <tt>null</tt> if the map contained no mapping for the key.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    V remove(K key);

    /**
     * Removes all of the mappings from this map.
     */
    void clear();

    /**
     * Returns the first (lowest) key currently in this map,
     * or {@code null} if the map is empty.
     *
     * @return the first (lowest) key currently in this map
     */
    K firstKey();

    /**
     * Returns the last (highest) key currently in this map,
     * or {@code null} if the map is empty.
     *
     * @return the last (highest) key currently in this map
     */
    K lastKey();

    /**
     * Returns a copy snapshot {@link Set} view of the keys contained in
     * this map. The set's iterator returns the keys whose order of iteration is
     * the descending order in which its entries are considered eligible for
     * retention, from the most-likely to be retained to the least-likely.
     *
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link java.util.ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator.

     * @return a set view of the keys contained in this map, sorted in
     *         ascending order
     */
    Set<K> keySet();
}
