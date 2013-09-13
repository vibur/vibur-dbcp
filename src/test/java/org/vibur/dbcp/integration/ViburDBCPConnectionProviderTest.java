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

package org.vibur.dbcp.integration;

import org.hibernate.Session;
import org.hibernate.connection.ConnectionProvider;
import org.hibernate.engine.SessionFactoryImplementor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;
import org.vibur.dbcp.ViburDBCPDataSourceImpl;
import org.vibur.dbcp.cache.StatementKey;
import org.vibur.dbcp.cache.ValueHolder;
import org.vibur.dbcp.common.HibernateTestUtil;
import org.vibur.dbcp.common.IntegrationTest;
import org.vibur.dbcp.model.Actor;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * Simple Hibernate integration test. See the prerequisites from {@link org.vibur.dbcp.AbstractDataSourceTest}
 * and from {@link HibernateTestUtil}.
 *
 * @author Simeon Malchev
 */
@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class ViburDBCPConnectionProviderTest {

    @Captor
    private ArgumentCaptor<StatementKey> key1, key2;

    @Test
    public void testSimpleSelectStatementNoStatementsCache() throws SQLException {
        Session session = HibernateTestUtil.getSessionFactoryNoStmtCache().getCurrentSession();
        try {
            executeAndVerifySimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleSelectStatementWithStatementsCache() throws SQLException {
        Session session = HibernateTestUtil.getSessionFactoryWithStmtCache().openSession();

        ConnectionProvider cp = ((SessionFactoryImplementor) session.getSessionFactory()).getConnectionProvider();
        ViburDBCPConnectionProvider vcp = (ViburDBCPConnectionProvider) cp;
        ViburDBCPDataSourceImpl ds = vcp.getDataSource();

        ConcurrentMap<StatementKey, ValueHolder<Statement>> mockedStatementCache =
            mock(ConcurrentMap.class, delegatesTo(ds.getStatementCache()));
        ds.setStatementCache(mockedStatementCache);

        executeAndVerifySimpleSelectInSession(session);
        // resources/hibernate-with-stmt-cache.cfg.xml defines pool with 1 connection only, that's why
        // the second session will use the same underlying connection.
        session = HibernateTestUtil.getSessionFactoryWithStmtCache().openSession();
        executeAndVerifySimpleSelectInSession(session);

        InOrder inOrder = inOrder(mockedStatementCache);
        inOrder.verify(mockedStatementCache).get(key1.capture());
        inOrder.verify(mockedStatementCache).putIfAbsent(same(key1.getValue()), any(ValueHolder.class));
        inOrder.verify(mockedStatementCache).get(key2.capture());

        assertEquals(key1.getValue(), key2.getValue());
        assertEquals("prepareStatement", key1.getValue().getMethod().getName());
        ValueHolder<Statement> valueHolder = mockedStatementCache.get(key1.getValue());
        assertFalse(valueHolder.inUse().get());
    }

    private void executeAndVerifySimpleSelectInSession(Session session) {
        try {
            executeAndVerifySimpleSelect(session);
        } catch (RuntimeException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void executeAndVerifySimpleSelect(Session session) {
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
