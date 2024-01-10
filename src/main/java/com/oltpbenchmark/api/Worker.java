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

package com.oltpbenchmark.api;

import com.oltpbenchmark.*;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oltpbenchmark.types.State.MEASURE;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final Logger ABORT_LOG = LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");

    private WorkloadState workloadState;
    private ResultStats resultStats;
    private final Statement currStatement;

    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final T benchmark;
    protected Connection conn = null;
    protected final WorkloadConfiguration configuration;
    protected final TransactionTypes transactionTypes;
    protected final Map<TransactionType, Procedure> procedures = new HashMap<>();
    protected final Map<String, Procedure> name_procedures = new HashMap<>();
    protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();

    private final Histogram<TransactionType> txnUnknown = new Histogram<>();
    private final Histogram<TransactionType> txnSuccess = new Histogram<>();
    private final Histogram<TransactionType> txnAbort = new Histogram<>();
    private final Histogram<TransactionType> txnRetry = new Histogram<>();
    private final Histogram<TransactionType> txnErrors = new Histogram<>();
    private final Histogram<TransactionType> txtRetryDifferent = new Histogram<>();

    private boolean seenDone = false;

    public Worker(T benchmark, int id) {
        this.id = id;
        this.benchmark = benchmark;
        this.configuration = this.benchmark.getWorkloadConfiguration();
        this.workloadState = this.configuration.getWorkloadState();
        this.currStatement = null;
        this.transactionTypes = this.configuration.getTransTypes();
        this.resultStats = new ResultStats(this.transactionTypes);

        if (!this.configuration.getNewConnectionPerTxn()) {
            try {
                this.conn = this.benchmark.makeConnection();
                this.conn.setAutoCommit(false);
                this.conn.setTransactionIsolation(this.configuration.getIsolationMode());
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to connect to database", ex);
            }
        }

        // Generate all the Procedures that we're going to need
        this.procedures.putAll(this.benchmark.getProcedures());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the BenchmarkModule managing this Worker
     */
    public final T getBenchmark() {
        return (this.benchmark);
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmark.getWorkloadConfiguration());
    }

    public final Random rng() {
        return (this.benchmark.rng());
    }

    public final long getRequests() {
        return resultStats.count();
    }

    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    public final ResultStats getStats() {
        return resultStats;
    }

    public final Procedure getProcedure(TransactionType type) {
        return (this.procedures.get(type));
    }

    @Deprecated
    public final Procedure getProcedure(String name) {
        return (this.name_procedures.get(name));
    }

    @SuppressWarnings("unchecked")
    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }

    public final Histogram<TransactionType> getTransactionUnknownHistogram() {
        return (this.txnUnknown);
    }

    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }

    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }

    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }

    public final Histogram<TransactionType> getTransactionRetryDifferentHistogram() {
        return (this.txtRetryDifferent);
    }

    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null) {
                this.currStatement.cancel();
            }
        } catch (SQLException e) {
            LOG.error("worker {} failed to cancel statement: {}", id, e.getMessage());
        }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        t.setName(this.toString());

        // In case of reuse reset the measurements
        resultStats = new ResultStats(this.transactionTypes);

        // Invoke initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this, ex);
        }

        // wait for start
        workloadState.blockForStart();

        boolean firstDelay = true;

        while (true) {

            // PART 1: Init and check if done

            State preState = workloadState.getGlobalState();

            // Do nothing
            if (preState == State.DONE) {
                if (!seenDone) {
                    // This is the first time we have observed that the
                    // test is done notify the global test state, then
                    // continue applying load
                    seenDone = true;
                    workloadState.signalDone();
                    break;
                }
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            workloadState.stayAwake();

            if (firstDelay) {
                firstDelay = false;
                int warmup = configuration.getWarmupTime();

                // Additional delay to avoid starting all threads simultaneously
                if (warmup > 0) {
                    long maxDelay = 2000 * warmup / 3;
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(maxDelay));
                        LOG.debug("Worker {} thread started", id);
                    } catch (InterruptedException e) {
                        LOG.error("Worker {} pre-start sleep interrupted", id, e);
                    }
                }
            }

            Phase prePhase = workloadState.getCurrentPhase();
            if (prePhase == null) {
                continue;
            }

            // Grab some work and update the state, in case it changed while we
            // waited.

            SubmittedProcedure pieceOfWork = workloadState.fetchWork();

            prePhase = workloadState.getCurrentPhase();
            if (prePhase == null) {
                continue;
            }

            preState = workloadState.getGlobalState();

            switch (preState) {
                case DONE, EXIT, LATENCY_COMPLETE -> {
                    // Once a latency run is complete, we wait until the next
                    // phase or until DONE.
                    LOG.warn("preState {} will continue...", preState);
                    continue;
                }
                default -> {
                }
                // Do nothing
            }

            // PART 3: Execute work

            TransactionType transactionType = getTransactionType(pieceOfWork, prePhase, preState, workloadState);

            if (!transactionType.equals(TransactionType.INVALID)) {

                // TODO: Measuring latency when not rate limited is ... a little
                // weird because if you add more simultaneous clients, you will
                // increase latency (queue delay) but we do this anyway since it is
                // useful sometimes

                // Wait before transaction if specified
                long preExecutionWaitInMillis = getPreExecutionWaitInMillis(transactionType);

                if (preExecutionWaitInMillis > 0) {
                    try {
                        LOG.debug("Worker {}: {} will sleep for {} ms before executing",
                            id, transactionType.getName(), preExecutionWaitInMillis);

                        Thread.sleep(preExecutionWaitInMillis);
                        LOG.debug("Worker {} woke up to execute {}", id, transactionType.getName());
                    } catch (InterruptedException e) {
                        LOG.error("Worker {} pre-execution sleep interrupted", id, e);
                    }
                }

                long start = System.nanoTime();

                TransactionStatus status = doWork(configuration.getDatabaseType(), transactionType);

                long end = System.nanoTime();

                // PART 4: Record results

                State postState = workloadState.getGlobalState();

                switch (postState) {
                    case MEASURE:
                        // Non-serial measurement. Only measure if the state both
                        // before and after was MEASURE, and the phase hasn't
                        // changed, otherwise we're recording results for a query
                        // that either started during the warmup phase or ended
                        // after the timer went off.
                        Phase postPhase = workloadState.getCurrentPhase();

                        if (postPhase == null) {
                            // Need a null check on postPhase since current phase being null is used in WorkloadState
                            // and ThreadBench as the indication that the benchmark is over. However, there's a race
                            // condition with postState not being changed from MEASURE to DONE yet, so we entered the
                            // switch. In this scenario, just break from the switch.
                            break;
                        }
                        if (preState == MEASURE && postPhase.getId() == prePhase.getId()) {
                            boolean isSuccess = status == TransactionStatus.SUCCESS ||
                                status == TransactionStatus.USER_ABORTED;
                            resultStats.addLatency(
                                transactionType.getId(),
                                start,
                                end,
                                isSuccess);
                            intervalRequests.incrementAndGet();
                        }
                        if (prePhase.isLatencyRun()) {
                            workloadState.startColdQuery();
                        }
                        break;
                    case COLD_QUERY:
                        // No recording for cold runs, but next time we will since
                        // it'll be a hot run.
                        if (preState == State.COLD_QUERY) {
                            workloadState.startHotQuery();
                        }
                        break;
                    default:
                        // Do nothing
                }


                // wait after transaction if specified
                long postExecutionWaitInMillis = getPostExecutionWaitInMillis(transactionType);

                if (postExecutionWaitInMillis > 0) {
                    try {
                        LOG.debug("Worker {} {} will sleep for {} ms after executing",
                            id, transactionType.getName(), postExecutionWaitInMillis);

                        Thread.sleep(postExecutionWaitInMillis);
                    } catch (InterruptedException e) {
                        LOG.error("Worker {} post-execution sleep interrupted", id, e);
                    }
                }
            }

            workloadState.finishedWork();
        }

        LOG.debug("Worker {} calling teardown", id);

        tearDown();
    }

    private TransactionType getTransactionType(SubmittedProcedure pieceOfWork, Phase phase, State state, WorkloadState workloadState) {
        TransactionType type = TransactionType.INVALID;

        try {
            type = transactionTypes.getType(pieceOfWork.getType());
        } catch (IndexOutOfBoundsException e) {
            if (phase.isThroughputRun()) {
                LOG.error("Worker {} thread tried executing disabled phase!", id);
                throw e;
            }
            if (phase.getId() == workloadState.getCurrentPhase().getId()) {
                switch (state) {
                    case WARMUP -> {
                        // Don't quit yet: we haven't even begun!
                        LOG.info("Worker {} [Serial] Resetting serial for phase.", id);
                        phase.resetSerial();
                    }
                    case COLD_QUERY, MEASURE -> {
                        // The serial phase is over. Finish the run early.
                        LOG.info("Worker {} [Serial] Updating workload state to {}.", id, State.LATENCY_COMPLETE);
                        workloadState.signalLatencyComplete();
                    }
                    default -> throw e;
                }
            }
        }

        return type;
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     *
     * @param databaseType TODO
     * @param transactionType TODO
     */
    protected final TransactionStatus doWork(DatabaseType databaseType, TransactionType transactionType) {
        TransactionStatus status = TransactionStatus.UNKNOWN;

        try {
            int retryCount = 0;
            int maxRetryCount = configuration.getMaxRetries();

            while (retryCount < maxRetryCount && this.workloadState.getGlobalState() != State.DONE) {

                status = TransactionStatus.UNKNOWN;

                if (this.conn == null) {
                    try {
                        LOG.debug("Worker {} opening a new connection", id);
                        this.conn = this.benchmark.makeConnection();
                        this.conn.setAutoCommit(false);
                        this.conn.setTransactionIsolation(this.configuration.getIsolationMode());
                    } catch (SQLException ex) {
                        LOG.debug("Worker {} failed to open a connection: {}", id, ex);
                        retryCount++;
                        continue;
                    }
                }

                try {

                    LOG.debug("Worker {} is attempting {}", id, transactionType);

                    status = this.executeWork(conn, transactionType);

                    LOG.debug("Worker {} completed {} with status {} and going to commit",
                        id, transactionType, status.name());

                    conn.commit();

                    break;

                } catch (UserAbortException ex) {
                    // TODO: probably check exception and retry if possible
                    try {
                        conn.rollback();
                        status = TransactionStatus.USER_ABORTED;
                    } catch (Exception e) {
                        LOG.warn(
                            "Worker {} failed to rollback transaction after UserAbortException ({}): {}",
                            id, ex.toString(), e.toString());

                        status = TransactionStatus.ERROR;
                    }

                    ABORT_LOG.debug("{} Aborted", transactionType, ex);


                    break;

                } catch (SQLException ex) {
                    // TODO: probably check exception and retry if possible
                    try {
                        LOG.debug("Worker {} rolled back transaction {}", id, transactionType);
                        conn.rollback();
                    } catch (Exception e) {
                        LOG.warn(
                            "Worker {} failed to rollback transaction after SQLException ({}): {}",
                            id, ex.toString(), e.toString());
                    }

                    if (isRetryable(ex)) {
                        LOG.debug(
                            "Worker {} Retryable SQLException occurred during [{}]... current retry attempt [{}], max retry attempts [{}], sql state [{}], error code [{}].",
                            id, transactionType, retryCount, maxRetryCount, ex.getSQLState(), ex.getErrorCode(), ex);

                        status = TransactionStatus.RETRY;

                        retryCount++;
                    } else {
                        LOG.warn(
                            "Worker {} SQLException occurred during [{}] and will not be retried... sql state [{}], error code [{}].",
                            id, transactionType, ex.getSQLState(), ex.getErrorCode(), ex);

                        status = TransactionStatus.ERROR;

                        break;
                    }

                } finally {
                    if (this.configuration.getNewConnectionPerTxn() && this.conn != null) {
                        try {
                            LOG.debug("Worker {} closing connection", id);
                            this.conn.close();
                            this.conn = null;
                            this.benchmark.returnConnection();
                        } catch (SQLException e) {
                            LOG.error("Worker {} connection couldn't be closed.", id, e);
                        }
                    }

                    switch (status) {
                        case UNKNOWN -> this.txnUnknown.put(transactionType);
                        case SUCCESS -> this.txnSuccess.put(transactionType);
                        case USER_ABORTED -> this.txnAbort.put(transactionType);
                        case RETRY -> this.txnRetry.put(transactionType);
                        case RETRY_DIFFERENT -> this.txtRetryDifferent.put(transactionType);
                        case ERROR -> this.txnErrors.put(transactionType);
                    }

                }

            }
        } catch (RuntimeException ex) {
            LOG.error("Worker {} Unexpected RuntimeException occurred (to be re-thrown) during [{}] on [{}]: {}",
                id, transactionType, databaseType.name(), ex.toString(), ex);
            throw ex;
        }

        return status;
    }

    private boolean isRetryable(SQLException ex) {

        String sqlState = ex.getSQLState();
        int errorCode = ex.getErrorCode();

        LOG.debug("Worker {} sql state [{}] and error code [{}]", id, sqlState, errorCode);

        if (sqlState == null) {
            return false;
        }

        // ------------------
        // MYSQL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
        // ------------------
        if (errorCode == 1213 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_DEADLOCK
            return true;
        } else if (errorCode == 1205 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_WAIT_TIMEOUT
            return true;
        }

        // ------------------
        // POSTGRES: https://www.postgresql.org/docs/current/errcodes-appendix.html
        // ------------------
        // Postgres serialization_failure
        return errorCode == 0 && sqlState.equals("40001");
    }

    /**
     * Optional callback that can be used to initialize the Worker right before
     * the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Invoke a single transaction for the given TransactionType
     *
     * @param conn    TODO
     * @param txnType TODO
     * @return TODO
     * @throws UserAbortException TODO
     * @throws SQLException       TODO
     */
    protected abstract TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     */
    public void tearDown() {
        if (!this.configuration.getNewConnectionPerTxn() && this.conn != null) {
            try {
                conn.close();
                this.conn = null;
                this.benchmark.returnConnection();
            } catch (SQLException e) {
                LOG.error("Worker {} connection couldn't be closed.", id, e);
            }
        }
    }

    public void initializeState() {
        this.workloadState = this.configuration.getWorkloadState();
    }

    protected long getPreExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

    protected long getPostExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

}
