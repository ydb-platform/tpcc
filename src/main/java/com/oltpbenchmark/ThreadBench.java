/*
 * Copyright 2020 by OLTPBenchmark Project
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
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.StringUtil;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ThreadBench implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadBench.class);

    private final BenchmarkState benchmarkState;
    private final List<? extends Worker<? extends BenchmarkModule>> workers;
    private final ArrayList<Thread> workerThreads;
    private final WorkloadConfiguration workConf;
    private final ResultStats resultStats;
    private final int intervalMonitor;
    private final Boolean useRealThreads;

    private ThreadBench(List<? extends Worker<? extends BenchmarkModule>> workers,
            WorkloadConfiguration workConf, int intervalMonitoring, Boolean useRealThreads) {
        this.workers = workers;
        this.workConf = workConf;
        this.workerThreads = new ArrayList<>(workers.size());
        this.intervalMonitor = intervalMonitoring;
        this.benchmarkState = new BenchmarkState(workers.size() + 1, workConf);
        this.resultStats = new ResultStats();
        this.useRealThreads = useRealThreads;

        for (Worker<?> worker : workers) {
            worker.setBenchmarkState(this.benchmarkState);
        }
    }

    public static Results runBenchmark(List<Worker<? extends BenchmarkModule>> workers,
            WorkloadConfiguration workConf, int intervalMonitoring, Boolean useRealThreads) {
        ThreadBench bench = new ThreadBench(workers, workConf, intervalMonitoring, useRealThreads);
        return bench.runBenchmark();
    }

    private void createWorkerThreads() {
        for (Worker<?> worker : workers) {
            Thread thread;
            if (useRealThreads) {
                thread = new Thread(worker);
            } else {
                thread = Thread.ofVirtual().unstarted(worker);
            }
            thread.setUncaughtExceptionHandler(this);
            thread.start();
            this.workerThreads.add(thread);
        }
    }

    private long finalizeWorkers(ArrayList<Thread> workerThreads) throws InterruptedException {
        long requests = 0;

        new WatchDogThread().start();

        for (int i = 0; i < workerThreads.size(); ++i) {

            // FIXME not sure this is the best solution... ensure we don't hang
            // forever, however we might ignore problems
            workerThreads.get(i).join(60000); // wait for 60second for threads
            // to terminate... hands otherwise

            /*
             * // CARLO: Maybe we might want to do this to kill threads that are
             * hanging... if (workerThreads.get(i).isAlive()) {
             * workerThreads.get(i).kill(); try { workerThreads.get(i).join(); }
             * catch (InterruptedException e) { } }
             */

            requests += workers.get(i).getRequests();

            LOG.debug("threadbench calling teardown");

            workers.get(i).tearDown();
        }

        return requests;
    }

    private Results runBenchmark() {
        this.createWorkerThreads();

        Phase phase = this.workConf.getPhase();
        LOG.info(phase.currentPhaseString());

        final long start = System.nanoTime();
        final long warmupStart = start;
        final long warmupEnd = warmupStart + phase.getWarmupTime() * 1000000000L;

        // Initialize the Monitor
        if (this.intervalMonitor > 0) {
            new MonitorThread(this.intervalMonitor).start();
        }

        // Allow workers to start work.
        benchmarkState.blockForStart();

        final long warmupS = (warmupEnd - warmupStart) / 1000000000;
        if (warmupS > 0) {
            LOG.info("{} :: Warming up for {}s", StringUtil.bold("WARMUP"), warmupS);
            while (System.nanoTime() < warmupEnd) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        final long stopAt = System.nanoTime() + phase.getTime() * 1000000000L;
        benchmarkState.startMeasure();

        LOG.info("{} :: Warmup complete, starting measurements.", StringUtil.bold("MEASURE"));

        // Main Loop
        while (benchmarkState.isWorkingOrMeasuring() && System.nanoTime() < stopAt) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        final long measureEnd = System.nanoTime();
        benchmarkState.stopWorkers();

        try {
            long requests = finalizeWorkers(this.workerThreads);

            for (Worker<?> w : workers) {
                resultStats.add(w.getStats());
            }

            Results results = new Results(measureEnd - start, requests, resultStats);

            // Compute transaction histogram
            Set<TransactionType> txnTypes = new HashSet<>();
            txnTypes.addAll(workConf.getTransTypes());
            txnTypes.remove(TransactionType.INVALID);

            results.getUnknown().putAll(txnTypes, 0);
            results.getSuccess().putAll(txnTypes, 0);
            results.getRetry().putAll(txnTypes, 0);
            results.getAbort().putAll(txnTypes, 0);
            results.getError().putAll(txnTypes, 0);
            results.getRetryDifferent().putAll(txnTypes, 0);

            for (Worker<?> w : workers) {
                results.getUnknown().putHistogram(w.getTransactionUnknownHistogram());
                results.getSuccess().putHistogram(w.getTransactionSuccessHistogram());
                results.getRetry().putHistogram(w.getTransactionRetryHistogram());
                results.getAbort().putHistogram(w.getTransactionAbortHistogram());
                results.getError().putHistogram(w.getTransactionErrorHistogram());
                results.getRetryDifferent().putHistogram(w.getTransactionRetryDifferentHistogram());
            }

            return (results);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // Here we handle the case in which one of our worker threads died
        LOG.error("Uncaught exception: {}", e.getMessage(), e);
        // We do not continue with the experiment. Instead, bypass rest of
        // phases that were left in the test and signal error state.
        // The rest of the workflow to finish the experiment remains the same,
        // and partial metrics will be reported (i.e., until failure happened).

        benchmarkState.signalError();
    }

    private class WatchDogThread extends Thread {
        {
            this.setDaemon(true);
        }

        @Override
        public void run() {
            Map<String, Object> m = new ListOrderedMap<>();
            LOG.info("Starting WatchDogThread");
            while (true) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    return;
                }

                m.clear();
                for (Thread t : workerThreads) {
                    m.put(t.getName(), t.isAlive());
                }
                LOG.info("Worker Thread Status:\n{}", StringUtil.formatMaps(m));
            }
        }
    }

    private class MonitorThread extends Thread {
        private final int intervalMonitor;

        {
            this.setDaemon(true);
        }

        /**
         * @param interval How long to wait between polling in milliseconds
         */
        MonitorThread(int interval) {
            this.intervalMonitor = interval;
        }

        @Override
        public void run() {
            LOG.info("Starting MonitorThread Interval [{}ms]", this.intervalMonitor);
            while (true) {
                try {
                    Thread.sleep(this.intervalMonitor);
                } catch (InterruptedException ex) {
                    return;
                }

                // Compute the last throughput
                long measuredRequests = 0;
                for (Worker<?> w : workers) {
                    measuredRequests += w.getAndResetIntervalRequests();
                }
                double seconds = this.intervalMonitor / 1000d;
                double tps = (double) measuredRequests / seconds;
                LOG.info("Throughput: {} txn/sec", tps);
            }
        }
    }
}
