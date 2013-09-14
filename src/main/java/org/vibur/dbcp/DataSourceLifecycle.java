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

package org.vibur.dbcp;

/**
 * Defines the {@link ViburDBCPDataSource} lifecycle operations.
 *
 * @author Simeon Malchev
 */
public interface DataSourceLifecycle {

    enum State {
        NEW,
        WORKING,
        TERMINATED
    }

    /**
     * Starts this DataSource. In order to be used, the implementing DataSource has to be
     * first initialised via call to one of the constructors and then started via call to this method.
     */
    void start();

    /**
     * Terminates this DataSource. Once terminated the DataSource cannot be more revived.
     */
    void terminate();

    /**
     * Returns this DataSource current state.
     *
     * @return see above
     */
    State getState();
}
