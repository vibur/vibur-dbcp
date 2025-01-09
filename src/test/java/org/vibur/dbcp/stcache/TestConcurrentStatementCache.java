package org.vibur.dbcp.stcache;

import java.util.concurrent.ConcurrentMap;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class TestConcurrentStatementCache extends ConcurrentStatementCache {

    private ConcurrentMap<StatementMethod, StatementHolder>[] cacheHolder;

    public TestConcurrentStatementCache(int maxSize) {
        super(maxSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    ConcurrentMap<StatementMethod, StatementHolder> buildStatementCache(int maxSize) {
        var mockedStatementCache = mock(ConcurrentMap.class, delegatesTo(super.buildStatementCache(maxSize)));
        cacheHolder = new ConcurrentMap[] {mockedStatementCache};
        return mockedStatementCache;
    }

    public ConcurrentMap<StatementMethod, StatementHolder> getMockedStatementCache() {
        return cacheHolder[0];
    }
}
