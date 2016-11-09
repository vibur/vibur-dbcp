/**
 * Copyright 2016 Simeon Malchev
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The JDBC Connector interface.
 *
 * @author Simeon Malchev
 */
public interface Connector {

    /**
     * Creates the physical (raw) JDBC connection to the database using the configured Driver or
     * external DataSource.
     *
     * @throws SQLException if the underlying SQL operation throws such
     */
    Connection connect() throws SQLException;

    ///////////////////////////////////////////////////////////////////////////////////////////////

    class Driver implements Connector {
        private final java.sql.Driver driver;
        private final String jdbcUrl;
        private final Properties driverProperties;

        public Driver(ViburConfig config, String username, String password) {
            this.driver = config.getDriver();
            this.jdbcUrl = config.getJdbcUrl();

            this.driverProperties = new Properties(config.getDriverProperties());
            driverProperties.setProperty("user", username);
            driverProperties.setProperty("password", password);
        }

        @Override
        public Connection connect() throws SQLException {
            return driver.connect(jdbcUrl, driverProperties);
        }
    }

    class DataSource implements Connector {
        private final javax.sql.DataSource externalDataSource;

        public DataSource(ViburConfig config) {
            this.externalDataSource = config.getExternalDataSource();
        }

        @Override
        public Connection connect() throws SQLException {
            return externalDataSource.getConnection();
        }
    }

    class DataSourceWithCredentials implements Connector {
        private final javax.sql.DataSource externalDataSource;
        private final String username;
        private final String password;

        public DataSourceWithCredentials(ViburConfig config, String username, String password) {
            this.externalDataSource = config.getExternalDataSource();
            this.username = username;
            this.password = password;
        }

        @Override
        public Connection connect() throws SQLException {
            return externalDataSource.getConnection(username, password);
        }
    }
}
