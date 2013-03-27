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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Simeon Malchev
 */

public class ValueHolder<V> {

    long order;

    private final V value;
    private final AtomicBoolean available;

    public ValueHolder(long order, V value, AtomicBoolean available) {
        this.order = order;
        this.value = value;
        this.available = available;
    }

    public V getValue() {
        return value;
    }

    public AtomicBoolean getAvailable() {
        return available;
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
