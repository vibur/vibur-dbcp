/**
 * Copyright 2017 Simeon Malchev
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

package org.vibur.dbcp.pool;

import org.vibur.dbcp.ViburConfig;
import org.vibur.dbcp.ViburDataSource;
import org.vibur.objectpool.util.TakenListener;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.vibur.dbcp.util.ViburUtils.getStackTraceAsString;

/**
 * @author Simeon Malchev
 */
public class ViburListener extends TakenListener<ConnHolder> {

    private static final ConnHolder[] NO_TAKEN_CONNS = {};

    private final ViburConfig config;

    public ViburListener(ViburConfig config) {
        super(config.getPoolMaxSize());
        this.config = config;
    }

    /**
     * See {@link org.vibur.dbcp.ViburDataSource#getTakenConnections} and {@link ViburDataSource#getTakenConnectionsStackTraces()}.
     */
    public TakenConnection[] getTakenConnections() {
        ConnHolder[] takenConns = getTaken(new ConnHolder[config.getPoolMaxSize()]);
        return Arrays.copyOf(takenConns, takenConns.length, TakenConnection[].class);
    }

    /**
     * See {@link ViburConfig#poolEnableConnectionTracking}, {@link ViburConfig#logTakenConnectionsOnTimeout}
     * and {@link ViburConfig#logAllStackTracesOnTimeout}.
     */
    public String getTakenConnectionsStackTraces() {
        ConnHolder[] takenConns = getTaken(new ConnHolder[config.getPoolMaxSize()]);
        if (takenConns.length == 0)
            return "";

        // sort the thread holding connection for the longest time on top
        Arrays.sort(takenConns, new Comparator<ConnHolder>() {
            @Override
            public int compare(ConnHolder h1, ConnHolder h2) {
                return Long.compare(h1.getTakenNanoTime(), h2.getTakenNanoTime());
            }
        });

        Map<Thread, StackTraceElement[]> currentStackTraces = getCurrentStackTraces(takenConns);
        long currentNanoTime = System.nanoTime();
        StringBuilder builder = new StringBuilder(takenConns.length * 8192);
        for (ConnHolder takenConn : takenConns) {
            Thread holdingThread = takenConn.getThread();
            builder.append("\n============\n").append(takenConn.rawConnection())
                    .append(", held for ").append(NANOSECONDS.toMillis(currentNanoTime - takenConn.getTakenNanoTime()));

            if (takenConn.getLastAccessNanoTime() == 0)
                builder.append(" ms, has not been accessed");
            else
                builder.append(" ms, last accessed before ").append(
                        NANOSECONDS.toMillis(currentNanoTime - takenConn.getLastAccessNanoTime())).append(" ms");

            builder.append(", taken by thread ").append(holdingThread.getName())
                    .append(", current thread state ").append(holdingThread.getState())
                    .append("\n\nThread stack trace at the moment when getting the Connection:\n")
                    .append(getStackTraceAsString(takenConn.getLocation().getStackTrace()));

            StackTraceElement[] currentStackTrace = currentStackTraces.remove(holdingThread);
            if (currentStackTrace != null && currentStackTrace.length > 0) {
                builder.append("\nThread stack trace at the current moment:\n")
                        .append(getStackTraceAsString(currentStackTrace));
            }
        }

        return addAllOtherStackTraces(builder, currentStackTraces).toString();
    }

    @Override
    protected ConnHolder[] getTaken(ConnHolder[] a) {
        ConnHolder[] takenConns =  super.getTaken(a);
        int size = 0;
        while (size < takenConns.length && takenConns[size] != null)
            size++;
        if (size == 0)
            return NO_TAKEN_CONNS;

        ConnHolder[] result = new ConnHolder[size];
        for (int i = 0; i < size; i++)
            result[i] = new ConnHolder(takenConns[i]);
        return result;
    }

    private static StringBuilder addAllOtherStackTraces(StringBuilder builder, Map<Thread, StackTraceElement[]> stackTraces) {
        if (stackTraces.isEmpty())
            return builder;

        builder.append("\n\n============ All other stack traces: ============\n\n");
        for (Map.Entry<Thread, StackTraceElement[]> entry : stackTraces.entrySet()) {
            Thread thread = entry.getKey();
            builder.append("\n============\n").append("Thread ").append(thread.getName())
                    .append(", state ").append(thread.getState());
            StackTraceElement[] currentStackTrace = entry.getValue();
            if (currentStackTrace.length > 0) {
                builder.append("\n\nThread stack trace at the current moment:\n")
                        .append(getStackTraceAsString(currentStackTrace));
            }
        }
        return builder;
    }

    private Map<Thread, StackTraceElement[]> getCurrentStackTraces(ConnHolder[] takenConns) {
        if (config.isLogAllStackTracesOnTimeout())
            return Thread.getAllStackTraces();

        Map<Thread, StackTraceElement[]> map = new HashMap<>(takenConns.length);
        for (ConnHolder takenConn : takenConns) {
            Thread holdingThread = takenConn.getThread();
            if (holdingThread.isAlive())
                map.put(holdingThread, holdingThread.getStackTrace());
        }
        return map;
    }
}
