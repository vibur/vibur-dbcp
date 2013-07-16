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
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Simeon Malchev
 */
@Category({IntegrationTest.class})
public class ViburDBCPConnectionProviderTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleStatementSelectNoStatementsCache() throws SQLException {
        Session session = HibernateTestUtil.getSessionFactory().getCurrentSession();
        try {
            session.beginTransaction();
            List<Actor> list = session.createQuery("from Actor where firstName = ?")
                .setParameter(0, "Renee").list();
            session.getTransaction().commit();
            assertEquals(2, list.size());
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        }
    }
}
