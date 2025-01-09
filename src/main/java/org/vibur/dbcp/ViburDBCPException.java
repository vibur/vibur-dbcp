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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * The base exception thrown by Vibur DBCP when an underlying error has occurred. This error could be a configuration
 * error, an initialization error, or an SQL error. Note that this is a {@code RuntimeException}; however, all methods
 * that throw this exception will explicitly declare it.
 *
 * @author Simeon Malchev
 */
public class ViburDBCPException extends RuntimeException {

    private static final Logger logger = LoggerFactory.getLogger(ViburDBCPException.class);

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

    public SQLException unwrapSQLException() {
        var cause = getCause();
        if (cause instanceof SQLException) {
            return (SQLException) cause;
        }

        logger.error("Unexpected exception cause", this);
        throw this; // not expected to happen
    }
}
