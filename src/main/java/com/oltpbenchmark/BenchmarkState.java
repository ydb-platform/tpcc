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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;

// shared between controlling ThreadBench thread and worker threads
public final class BenchmarkState {
    public enum State {
        WARMUP, MEASURE, DONE, EXIT, ERROR
    }

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkState.class);

    private final WorkloadConfiguration workloadConf;
    private final long testStartNs;
    private final CountDownLatch startBarrier;
    private final AtomicInteger notDoneCount;

    private final AtomicReference<State> state = new AtomicReference<>(State.WARMUP);

    /**
     * @param numThreads number of threads involved in the test: including the
     *                   master thread.
     */
    public BenchmarkState(int numThreads, WorkloadConfiguration workloadConf) {
        this.workloadConf = workloadConf;
        this.startBarrier = new CountDownLatch(numThreads);
        this.notDoneCount = new AtomicInteger(numThreads);

        this.testStartNs = System.nanoTime();
    }

    public SubmittedProcedure fetchWork() {
        if (getState() == State.EXIT || getState() == State.DONE) {
            return null;
        }

        return new SubmittedProcedure(workloadConf.getPhase().chooseTransaction());
    }

    // Protected by this

    public long getTestStartNs() {
        return testStartNs;
    }

    public State getState() {
        return state.get();
    }

    /**
     * Wait for all threads to call this. Returns once all the threads have
     * entered.
     */
    public void blockForStart() {
        startBarrier.countDown();
        try {
            startBarrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startMeasure() {
        state.set(State.MEASURE);
    }

    public void signalError() {
        notDoneCount.decrementAndGet();
        state.set(State.ERROR);
    }

    public boolean isError() {
        return getState() == State.ERROR;
    }

    public boolean isWorkingOrMeasuring() {
        return getState() == State.WARMUP || getState() == State.MEASURE;
    }

    public boolean isMeasuring() {
        return getState() == State.MEASURE;
    }

    public void workerFinished() {
        notDoneCount.decrementAndGet();
    }

    public void stopWorkers() {
        int waitCount = notDoneCount.decrementAndGet();
        if (waitCount > 0) {
            LOG.debug(String.format("%d workers are not done. Waiting until they finish", waitCount));
        }

        if (getState() != State.ERROR) {
            // might be a minor race here, but not a problem
            state.set(State.DONE);
        }

        while (notDoneCount.get() > 0) {
            Thread.yield();
        }

        LOG.debug("Workers stopped");
    }
}