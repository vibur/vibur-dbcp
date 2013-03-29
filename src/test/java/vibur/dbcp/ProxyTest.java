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

package vibur.dbcp;

import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test uses the <a href="http://dev.mysql.com/doc/sakila/en/">Sakila sample database</a>.
 * The database has to be installed and setup as described in the previous link.
 *
 * @author Simeon Malchev
 */
public class ProxyTest {

    private ViburDBCPDataSource viburDS = null;

    @After
    public void tearDown() throws Exception {
        viburDS.shutdown();
    }

    @Test
    public void testSimpleSelectsNoStatementsCache() throws SQLException {
        DataSource ds = getSimpleDataSource();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ds.getConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            statement = connection.createStatement();
            resultSet = statement.executeQuery("select * from actor where first_name='CHRISTIAN'");

            Set<String> lastNames = new HashSet<String>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
            int count = 0;
            while (resultSet.next()) {
                ++count;
                String lastName = resultSet.getString("last_name");
                assertTrue(lastNames.remove(lastName));
            }
            assertEquals(3, count);
        } finally {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    private DataSource getSimpleDataSource() {
        viburDS = new ViburDBCPDataSource();

        viburDS.setDriverClassName("com.mysql.jdbc.Driver");
        viburDS.setJdbcUrl("jdbc:mysql://localhost/sakila");
        viburDS.setUsername("root");
        viburDS.setPassword("root");

        viburDS.setPoolInitialSize(2);
        viburDS.setValidateOnRestore(true);

        viburDS.setLogStatementsEnabled(true);
        viburDS.setQueryExecuteTimeLimitInMs(1);

        viburDS.start();

        return viburDS;
    }
}
