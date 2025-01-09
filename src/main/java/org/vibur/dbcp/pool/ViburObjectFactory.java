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

package org.vibur.dbcp.pool;

import org.vibur.dbcp.ViburDBCPException;
import org.vibur.objectpool.PoolObjectFactory;

/**
 * Defines the operations that are specific to Vibur {@link ConnectionFactory}. These are the factory's
 * version manipulation methods and the connection creation with credentials method.
 *
 * @author Simeon Malchev
 */
public interface ViburObjectFactory extends PoolObjectFactory<ConnHolder> {

    /**
     * {@inheritDoc}
     *
     * @throws org.vibur.dbcp.ViburDBCPException if cannot create or initialize the raw JDBC Connection
     */
    @Override
    ConnHolder create() throws ViburDBCPException;

    /**
     * Creates a new {@link ConnHolder} object using the given connector. This object is presumed to be
     * ready (and valid) for immediate use. This method <b>never</b> return {@code null}.
     *
     * @param connector the JDBC connector; it contains the database credentials
     * @throws ViburDBCPException if cannot create or initialize the raw JDBC Connection
     */
    ConnHolder create(Connector connector) throws ViburDBCPException;

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Gets the current version value.
     *
     * @return the current version value
     */
    int version();

    /**
     * Atomically sets the version value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect the expected version value
     * @param update the new version value
     * @return true if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    boolean compareAndSetVersion(int expect, int update);
}
