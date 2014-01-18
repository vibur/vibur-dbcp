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
 * The exception which is thrown from different Vibur DBCP classes/methods when an underlying error has happened.
 * Note that this is a RuntimeException however all methods which throw this exception will explicitly declare it.
 *
 * @author Simeon Malchev
 */
public class ViburDBCPException extends RuntimeException {

    public ViburDBCPException() {
        super();
    }

    public ViburDBCPException(String message) {
        super(message);
    }

    public ViburDBCPException(String message, Throwable cause) {
        super(message, cause);
    }

    public ViburDBCPException(Throwable cause) {
        super(cause);
    }
}
