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
import org.vibur.objectpool.util.TakenListener;

import java.util.Arrays;

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
     * See {@link org.vibur.dbcp.ViburDataSource#getTakenConnections}.
     */
    public TakenConnection[] getTakenConnections() {
        ConnHolder[] takenConns = getTaken(new ConnHolder[config.getPoolMaxSize()]);
        if (takenConns.length == 0)
            return NO_TAKEN_CONNECTIONS;

        return Arrays.copyOf(takenConns, takenConns.length, TakenConnection[].class);
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
}
