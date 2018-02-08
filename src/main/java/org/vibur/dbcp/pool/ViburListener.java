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

    public static final TakenConnection[] NO_TAKEN_CONNECTIONS = {};
    private static final ConnHolder[] NO_TAKEN_CONN_HOLDERS = {};

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
        if (takenConns.length == 0)
            return NO_TAKEN_CONNECTIONS;

        return Arrays.copyOf(takenConns, takenConns.length, TakenConnection[].class);
    }

    /**
     * See {@link ViburConfig#poolEnableConnectionTracking}, {@link ViburConfig#logTakenConnectionsOnTimeout}
     * and {@link ViburConfig#logAllStackTracesOnTimeout}.
     */
    public String getTakenConnectionsStackTraces() {
        TakenConnection[] takenConns = getTakenConnections();
        if (takenConns.length == 0)
            return "";

        // sort the thread holding connection for the longest time on top
        Arrays.sort(takenConns, new Comparator<TakenConnection>() {
            @Override
            public int compare(TakenConnection t1, TakenConnection t2) {
                return Long.compare(t1.getTakenNanoTime(), t2.getTakenNanoTime());
            }
        });

        long currentNanoTime = System.nanoTime();
        StringBuilder builder = new StringBuilder(takenConns.length * 8192);
        Map<Thread, StackTraceElement[]> currentStackTraces = getCurrentStackTraces(takenConns);
        for (int i = 0; i < takenConns.length; i++) {
            Thread holdingThread = takenConns[i].getThread();
            builder.append("\n============\n(").append(i + 1).append('/').append(takenConns.length).append("), ")
                    .append(takenConns[i].getProxyConnection())
                    .append(", held for ").append(NANOSECONDS.toMillis(currentNanoTime - takenConns[i].getTakenNanoTime()));

            if (takenConns[i].getLastAccessNanoTime() == 0)
                builder.append(" ms, has not been accessed");
            else
                builder.append(" ms, last accessed before ").append(
                        NANOSECONDS.toMillis(currentNanoTime - takenConns[i].getLastAccessNanoTime())).append(" ms");

            builder.append(", taken by thread ").append(holdingThread.getName())
                    .append(", current thread state ").append(holdingThread.getState())
                    .append("\n\nThread stack trace at the moment when getting the Connection:\n")
                    .append(getStackTraceAsString(config.getLogLineRegex(), takenConns[i].getLocation().getStackTrace()));

            StackTraceElement[] currentStackTrace = currentStackTraces.remove(holdingThread);
            if (currentStackTrace != null && currentStackTrace.length > 0) {
                builder.append("\nThread stack trace at the current moment:\n")
                        .append(getStackTraceAsString(config.getLogLineRegex(), currentStackTrace));
            }
        }

        return addAllOtherStackTraces(builder, currentStackTraces).toString();
    }

    @Override
    protected ConnHolder[] getTaken(ConnHolder[] a) {
        ConnHolder[] takenConns = super.getTaken(a);
        int size = 0;
        while (size < takenConns.length && takenConns[size] != null)
            size++;
        if (size == 0)
            return NO_TAKEN_CONN_HOLDERS;

        ConnHolder[] result = new ConnHolder[size];
        for (int i = 0; i < size; i++)
            result[i] = new ConnHolder(takenConns[i]);
        return result;
    }

    private StringBuilder addAllOtherStackTraces(StringBuilder builder, Map<Thread, StackTraceElement[]> stackTraces) {
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
                        .append(getStackTraceAsString(config.getLogLineRegex(), currentStackTrace));
            }
        }
        return builder;
    }

    private Map<Thread, StackTraceElement[]> getCurrentStackTraces(TakenConnection[] takenConns) {
        if (config.isLogAllStackTracesOnTimeout())
            return Thread.getAllStackTraces();

        Map<Thread, StackTraceElement[]> map = new HashMap<>(takenConns.length);
        for (TakenConnection takenConn : takenConns) {
            Thread holdingThread = takenConn.getThread();
            if (holdingThread.isAlive())
                map.put(holdingThread, holdingThread.getStackTrace());
        }
        return map;
    }
}
