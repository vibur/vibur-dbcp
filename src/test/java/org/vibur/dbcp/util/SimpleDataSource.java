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

package org.vibur.dbcp.util;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * @author Simeon Malchev
 */
public class SimpleDataSource implements DataSource {

    private final String jdbcUrl;

    public SimpleDataSource(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /** {@inheritDoc} */
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    /** {@inheritDoc} */
    public Connection getConnection(String username, String password) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /** {@inheritDoc} */
    public PrintWriter getLogWriter() throws SQLException {
        return DriverManager.getLogWriter();
    }

    /** {@inheritDoc} */
    public void setLogWriter(PrintWriter out) throws SQLException {
        DriverManager.setLogWriter(out);
    }

    /** {@inheritDoc} */
    public void setLoginTimeout(int seconds) throws SQLException {
        DriverManager.setLoginTimeout(seconds);
    }

    /** {@inheritDoc} */
    public int getLoginTimeout() throws SQLException {
        return DriverManager.getLoginTimeout();
    }

    /** {@inheritDoc} */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
