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

package org.vibur.dbcp.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPDataSource;
import org.vibur.dbcp.ViburDBCPException;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Simeon Malchev
 */
public class ViburDBCPGetConnectionTestPerf {

    private static final Logger logger = LoggerFactory.getLogger(ViburDBCPGetConnectionTestPerf.class);

    // pool metrics:
    private static final int INITIAL_SIZE = 50;
    private static final int MAX_SIZE = 200;
    private static final long TIMEOUT_MS = 2000;
    private static final boolean FAIR = true;

    // threads metrics:
    private static final int ITERATIONS = 100;
    private static final int THREADS_COUNT = 500;
    private static final long DO_WORK_FOR_MS = 2;

    public static void main(String[] args) throws InterruptedException, ViburDBCPException {

        // Creates a DataSource with an INITIAL_SIZE and a MAX_SIZE, and starts a THREADS_COUNT threads
        // where each thread executes ITERATIONS times the following code:
        //
        //     Connection connection = ds.getConnection();
        //     doWork(DO_WORK_FOR_MS);
        //     connection.close();
        //
        // Each getConnection() call has a TIMEOUT_MS and the number of unsuccessful calls is recorded.
        // Measures and reports the total time taken by the test in ms.

        var ds = createDataSource();
        ds.start();

        var errors = new AtomicInteger(0);

        var startSignal = new CountDownLatch(1);
        var readySignal = new CountDownLatch(THREADS_COUNT);
        var doneSignal = new CountDownLatch(THREADS_COUNT);

        for (var i = 0; i < THREADS_COUNT; i++) {
            var thread = new Thread(new Worker(ds, errors, DO_WORK_FOR_MS, readySignal, startSignal, doneSignal));
            thread.start();
        }

        readySignal.await();
        var startNanoTime = System.nanoTime();
        startSignal.countDown();
        doneSignal.await();

        System.out.printf("Total execution time %f ms, unsuccessful takes %d.%n",
            (System.nanoTime() - startNanoTime) * 1e-6, errors.get());

        ds.close();
    }

    private static final class Worker implements Runnable {
        private final ViburDBCPDataSource ds;
        private final AtomicInteger errors;
        private final long millis;

        private final CountDownLatch readySignal;
        private final CountDownLatch startSignal;
        private final CountDownLatch doneSignal;

        private Worker(ViburDBCPDataSource ds, AtomicInteger errors, long millis,
                       CountDownLatch readySignal, CountDownLatch startSignal, CountDownLatch doneSignal) {
            this.ds = ds;
            this.errors = errors;
            this.millis = millis;
            this.startSignal = startSignal;
            this.readySignal = readySignal;
            this.doneSignal = doneSignal;
        }

        @Override
        public void run() {
            try {
                readySignal.countDown();
                startSignal.await();

                for (var i = 0; i < ITERATIONS; i++) {
                    try {
                        var connection = ds.getConnection();
                        doWork(millis);
                        connection.close();
                    } catch (SQLException e) {
                        logger.error(e.toString());
                        errors.incrementAndGet();
                    }
                }
            } catch (InterruptedException ignored) {
                errors.incrementAndGet();
            } finally {
                doneSignal.countDown();
            }
        }
    }

    private static ViburDBCPDataSource createDataSource() {
        var ds = new ViburDBCPDataSource();
        ds.setJdbcUrl("jdbc:hsqldb:mem:sakila;shutdown=false");
        ds.setUsername("sa");
        ds.setPassword("");

        ds.setPoolInitialSize(INITIAL_SIZE);
        ds.setPoolMaxSize(MAX_SIZE);
        ds.setConnectionTimeoutInMs(TIMEOUT_MS);
        ds.setPoolFair(FAIR);
        return ds;
    }

    private static void doWork(long millis) {
        if (millis <= 0) {
            return;
        }

        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) { }
    }
}
