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

package vibur.dbcp.integration;

import org.hibernate.Session;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import vibur.dbcp.common.IntegrationTest;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple Hibernate integration test.
 *
 * Also see the first 2 prerequisites for running the test from {@link vibur.dbcp.ViburDBCPDataSourceTest}
 * and the resources/hibernate-*-stmt-cache.cfg.xml configuration files.
 *
 * @author Simeon Malchev
 */
@Category({IntegrationTest.class})
public class ViburDBCPConnectionProviderTest {

    @Test
    public void testSimpleStatementSelectNoStatementsCache() throws SQLException {
        Session session = HibernateTestUtil.getSessionFactoryNoStmtCache().getCurrentSession();
        try {
            executeSimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        }
    }

    @Test
    public void testSimpleStatementSelectWithStatementsCache() throws SQLException {
        openAndCloseSession();
        // hibernate-with-stmt-cache.cfg.xml defines pool with only 1 connection, that's why
        // the second session will hit the same underlying connection.
        openAndCloseSession();
    }

    private void openAndCloseSession() {
        Session session = HibernateTestUtil.getSessionFactoryWithStmtCache().openSession();
        try {
            executeSimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void executeSimpleSelect(Session session) {
        session.beginTransaction();
        List<Actor> list = session.createQuery("from Actor where firstName = ?")
            .setParameter(0, "CHRISTIAN").list();
        session.getTransaction().commit();

        Set<String> expectedLastNames = new HashSet<String>(Arrays.asList("GABLE", "AKROYD", "NEESON"));
        assertEquals(expectedLastNames.size(), list.size());
        for (Actor actor : list) {
            assertTrue(expectedLastNames.remove(actor.getLastName()));
        }
    }
}
