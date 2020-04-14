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
import org.vibur.dbcp.stcache.TestClhmStatementCache;
import org.vibur.dbcp.stcache.StatementHolder;
import org.vibur.dbcp.stcache.StatementMethod;
import org.vibur.dbcp.util.HsqldbUtils;
import org.vibur.dbcp.util.SimpleDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;

/**
 * Abstract JDBC integration super test.
 *
 * @author Simeon Malchev
 */
public abstract class AbstractDataSourceTest {

    protected static final String PROPERTIES_FILE = "src/test/resources/vibur-dbcp-test.properties";
    protected static final int POOL_INITIAL_SIZE = 2;
    protected static final int POOL_MAX_SIZE = 10;
    protected static final int CONNECTION_TIMEOUT_MS = 5000;

    private static String jdbcUrl;
    private static String username;
    private static String password;

    protected AbstractDataSourceTest() { }

    @BeforeClass
    public static void deployDatabaseSchemaAndData() throws IOException, SqlToolError, SQLException {
        Properties properties = loadProperties();
        jdbcUrl = properties.getProperty("jdbcUrl");
        username = properties.getProperty("username");
        password = properties.getProperty("password");
        HsqldbUtils.deployDatabaseSchemaAndData(jdbcUrl, username, password);
    }

    protected static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(PROPERTIES_FILE));
        return properties;
    }

    private ViburDBCPDataSource dataSource = null;

    @After
    public void terminateDataSource() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
    }

    protected ViburDBCPDataSource createDataSourceNoStatementsCache() throws ViburDBCPException {
        dataSource = new ViburDBCPDataSource();

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolInitialSize(POOL_INITIAL_SIZE);
        dataSource.setPoolMaxSize(POOL_MAX_SIZE);
        dataSource.setConnectionTimeoutInMs(CONNECTION_TIMEOUT_MS);

        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(0);
        dataSource.setLogConnectionLongerThanMs(0);
        dataSource.setLogLargeResultSet(2);

        dataSource.setValidateTimeoutInSeconds(1);
        dataSource.setClearSQLWarnings(true);

        dataSource.start();

        return dataSource;
    }

    protected ViburDBCPDataSource createDataSourceWithTracking() throws ViburDBCPException {
        dataSource = new ViburDBCPDataSource();

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolInitialSize(POOL_INITIAL_SIZE);
        dataSource.setPoolMaxSize(POOL_MAX_SIZE);
        dataSource.setConnectionTimeoutInMs(CONNECTION_TIMEOUT_MS);

        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(0);
        dataSource.setLogConnectionLongerThanMs(0);
        dataSource.setLogLargeResultSet(2);

        dataSource.setPoolEnableConnectionTracking(true);

        dataSource.start();

        return dataSource;
    }

    protected ViburDBCPDataSource createDataSourceWithStatementsCache() throws ViburDBCPException {
        dataSource = new ViburDBCPDataSource();

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolInitialSize(POOL_INITIAL_SIZE);
        dataSource.setPoolMaxSize(POOL_MAX_SIZE);
        dataSource.setConnectionTimeoutInMs(CONNECTION_TIMEOUT_MS);

        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(1);
        dataSource.setStatementCacheMaxSize(1);

        dataSource.start();

        return dataSource;
    }

    protected ViburDBCPDataSource createDataSourceFromExternalDataSource() throws ViburDBCPException {
        dataSource = new ViburDBCPDataSource();

        dataSource.setExternalDataSource(new SimpleDataSource(jdbcUrl));
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolInitialSize(POOL_INITIAL_SIZE);
        dataSource.setPoolMaxSize(POOL_MAX_SIZE);
        dataSource.setConnectionTimeoutInMs(CONNECTION_TIMEOUT_MS);

        dataSource.setConnectionIdleLimitInSeconds(120);

        dataSource.setLogQueryExecutionLongerThanMs(0);
        dataSource.setLogConnectionLongerThanMs(0);

        dataSource.start();

        return dataSource;
    }

    protected ViburDBCPDataSource createDataSourceNotStarted() throws ViburDBCPException {
        dataSource = new ViburDBCPDataSource();

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setPoolInitialSize(POOL_INITIAL_SIZE);
        dataSource.setPoolMaxSize(POOL_MAX_SIZE);
        dataSource.setConnectionTimeoutInMs(CONNECTION_TIMEOUT_MS);

        dataSource.setConnectionIdleLimitInSeconds(120);
        dataSource.setLogQueryExecutionLongerThanMs(-1);

        return dataSource;
    }

    public static ConcurrentMap<StatementMethod, StatementHolder> mockStatementCache(ViburDBCPDataSource ds) {
        TestClhmStatementCache testCache = new TestClhmStatementCache(ds.getStatementCacheMaxSize());
        ds.setStatementCache(testCache);
        return testCache.getMockedStatementCache();
    }
}
