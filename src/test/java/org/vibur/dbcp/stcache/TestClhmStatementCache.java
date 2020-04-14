package org.vibur.dbcp.stcache;

import java.util.concurrent.ConcurrentMap;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class TestClhmStatementCache extends ClhmStatementCache {

    private ConcurrentMap<StatementMethod, StatementHolder>[] cacheHolder;

    public TestClhmStatementCache(int maxSize) {
        super(maxSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    ConcurrentMap<StatementMethod, StatementHolder> buildStatementCache(int maxSize) {
        ConcurrentMap<StatementMethod, StatementHolder> mockedStatementCache =
                mock(ConcurrentMap.class, delegatesTo(super.buildStatementCache(maxSize)));
        cacheHolder = new ConcurrentMap[] {mockedStatementCache};
        return mockedStatementCache;
    }

    public ConcurrentMap<StatementMethod, StatementHolder> getMockedStatementCache() {
        return cacheHolder[0];
    }
}
