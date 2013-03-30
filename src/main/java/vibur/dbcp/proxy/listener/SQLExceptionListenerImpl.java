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

package vibur.dbcp.proxy.listener;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Simeon Malchev
 */
public class SQLExceptionListenerImpl implements SQLExceptionListener {

    private Queue<Throwable> queue = new ConcurrentLinkedQueue<Throwable>();

    public void addException(Throwable throwable) {
        queue.offer(throwable);
    }

    public List<Throwable> getExceptions() {
        return new LinkedList<Throwable>(queue);
    }
}
