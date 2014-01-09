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

import org.hsqldb.cmdline.SqlToolError;
import org.junit.After;
import org.junit.BeforeClass;
import org.vibur.dbcp.common.HsqldbUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Abstract JDBC integration super test.
 *
 * @author Simeon Malchev
 */
public abstract class AbstractDataSourceTest {

    @BeforeClass
    public static void deployDatabaseSchemaAndData() throws IOException, SqlToolError {
        Properties properties = loadProperties();
        HsqldbUtils.deployDatabaseSchemaAndData(properties.getProperty("jdbcUrl"),
            properties.getProperty("username"), properties.getProperty("password"));
    }

    private ViburDBCPDataSource dataSource = null;

    @After
    public void terminateDataSource() {
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
        dataSource.setLogConnectionLongerThanMs(0);

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

    protected static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/test/resources/vibur-dbcp-test.properties"));
        return properties;
    }
}
