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

import org.junit.After;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Abstract JDBC integration super test. Prerequisites for running all tests which inherit from it:
 *
 * <p>1. Install and run MySQL server.
 *
 * <p>2. Install <a href="http://dev.mysql.com/doc/sakila/en/">Sakila Sample Database</a>
 * as described in the link.
 *
 * <p>3. Configure appropriately the database connection parameters in resources/vibur-dbcp-test.properties.
 *
 * @author Simeon Malchev
 */
public abstract class AbstractDataSourceTest {

    private ViburDBCPDataSource dataSource = null;

    @After
    public void cleanup() {
        if (dataSource != null) {
            dataSource.terminate();
            dataSource = null;
        }
    }

    protected ViburDBCPDataSource createDataSourceNoStatementsCache() throws IOException {
        dataSource = new ViburDBCPDataSource();

        Properties properties = loadProperties();
        dataSource.setDriverClassName(properties.getProperty("driverClassName"));
        dataSource.setJdbcUrl(properties.getProperty("jdbcUrl"));
        dataSource.setUsername(properties.getProperty("username"));
        dataSource.setPassword(properties.getProperty("password"));

        dataSource.setPoolInitialSize(2);
        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(0);
        dataSource.setLogCreateConnectionLongerThanMs(0);

        dataSource.start();

        return dataSource;
    }

    protected ViburDBCPDataSource createDataSourceWithStatementsCache() throws IOException {
        dataSource = new ViburDBCPDataSource();

        Properties properties = loadProperties();
        dataSource.setDriverClassName(properties.getProperty("driverClassName"));
        dataSource.setJdbcUrl(properties.getProperty("jdbcUrl"));
        dataSource.setUsername(properties.getProperty("username"));
        dataSource.setPassword(properties.getProperty("password"));

        dataSource.setPoolInitialSize(2);
        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(1);
        dataSource.setStatementCacheMaxSize(10);

        dataSource.start();

        return dataSource;
    }

    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/test/resources/vibur-dbcp-test.properties"));
        return properties;
    }
}
