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

/**
 * Defines the operations for a generic {@code versioned} object, which is a stateful object
 * that has a version associated with its state.
 *
 * @author Simeon Malchev
 */
public interface VersionedObject {

    /**
     * Gets the current version value.
     *
     * @return the current version value
     */
    public int getVersion();

    /**
     * Sets to the given version value.
     *
     * @param newValue the new version value
     */
    public void setVersion(int newValue);

    /**
     * Atomically sets the version value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect the expected version value
     * @param update the new version value
     * @return true if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public boolean compareAndSetVersion(int expect, int update);
}
