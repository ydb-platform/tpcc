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
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.jdbc.exception.YdbStatusable;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final Logger ABORT_LOG = LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");

    private static final AtomicInteger WORKERS_SLEEPING = new AtomicInteger(0);
    private static final AtomicInteger WORKERS_WORKING = new AtomicInteger(0);

    private static final Gauge WORKERS_SLEEPING_GAUGE = Gauge.builder("workers", WORKERS_SLEEPING, AtomicInteger::get)
            .tag("state", "sleeping")
            .register(Metrics.globalRegistry);

    private static final Gauge WORKERS_WORKING_GAUGE = Gauge.builder("workers", WORKERS_WORKING, AtomicInteger::get)
            .tag("state", "working")
            .register(Metrics.globalRegistry);

    private static final Counter.Builder TRANSACTIONS = Counter.builder("transactions");
    private static final Counter.Builder EXECUTIONS = Counter.builder("executions");
    private static final Counter.Builder YDB_ERRORS = Counter.builder("ydb_errors");
    private static final Counter.Builder CONNECTION_ERRORS = Counter.builder("connection_errors");

    private static final Timer.Builder TRANSACTION_DURATION = Timer.builder("transaction")
            .serviceLevelObjectives(
                    Duration.ofMillis(1),
                    Duration.ofMillis(2),
                    Duration.ofMillis(4),
                    Duration.ofMillis(8),
                    Duration.ofMillis(16),
                    Duration.ofMillis(32),
                    Duration.ofMillis(64),
                    Duration.ofMillis(128),
                    Duration.ofMillis(256),
                    Duration.ofMillis(512),
                    Duration.ofMillis(1024),
                    Duration.ofMillis(2048),
                    Duration.ofMillis(4096),
                    Duration.ofMillis(8192),
                    Duration.ofMillis(16384),
                    Duration.ofMillis(32768),
                    Duration.ofMillis(65536)
            )
            .publishPercentiles();

    private static final Timer.Builder EXECUTION_DURATION = Timer.builder("execution")
            .serviceLevelObjectives(
                    Duration.ofMillis(1),
                    Duration.ofMillis(2),
                    Duration.ofMillis(4),
                    Duration.ofMillis(8),
                    Duration.ofMillis(16),
                    Duration.ofMillis(32),
                    Duration.ofMillis(64),
                    Duration.ofMillis(128),
                    Duration.ofMillis(256),
                    Duration.ofMillis(512),
                    Duration.ofMillis(1024),
                    Duration.ofMillis(2048),
                    Duration.ofMillis(4096),
                    Duration.ofMillis(8192),
                    Duration.ofMillis(16384),
                    Duration.ofMillis(32768),
                    Duration.ofMillis(65536)
            )
            .publishPercentiles();

    private ResultStats resultStats;

    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final T benchmark;
    protected Connection conn = null;
    protected final WorkloadConfiguration configuration;
    protected BenchmarkState benchmarkState;
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

    public Worker(T benchmark, int id) {
        this.id = id;
        this.benchmark = benchmark;
        this.configuration = this.benchmark.getWorkloadConfiguration();
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

    public void setBenchmarkState(BenchmarkState state) {
        this.benchmarkState = state;
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
        benchmarkState.blockForStart();

        // Additional delay to avoid starting all threads simultaneously
        final int warmup = configuration.getWarmupTime();
        if (warmup > 0) {
            int maxDelayMs = (int)(1000 * warmup * 2 / 3);
            int delayMs = ThreadLocalRandom.current().nextInt(maxDelayMs);
            LOG.debug("Worker {} will sleep for {} s before starting", id, (int)(delayMs / 1000));
            try {
                Thread.sleep(delayMs);
                LOG.debug("Worker {} thread started", id);
            } catch (InterruptedException e) {
                LOG.error("Worker {} pre-start sleep interrupted", id, e);
            }
        }

        while (benchmarkState.isWorkingOrMeasuring()) {
            SubmittedProcedure pieceOfWork = benchmarkState.fetchWork();
            if (pieceOfWork == null) {
                LOG.debug("Worker {} thread got null work", id);
                break;
            }

            TransactionType transactionType;
            try {
                transactionType = transactionTypes.getType(pieceOfWork.getType());

                // just sanity check
                if (transactionType.equals(TransactionType.INVALID)) {
                    LOG.error("Worker {} thread got invalid work", id);
                    throw new IndexOutOfBoundsException("Invalid transaction type");
                }
            } catch (IndexOutOfBoundsException e) {
                LOG.error("Worker {} thread tried executing disabled phase!", id);
                throw e;
            }

            long preExecutionWaitInMillis = getPreExecutionWaitInMillis(transactionType);
            if (preExecutionWaitInMillis > 0) {
                try {
                    LOG.debug("Worker {}: {} will sleep for {} ms before executing",
                        id, transactionType.getName(), preExecutionWaitInMillis);

                    WORKERS_SLEEPING.incrementAndGet();
                    Thread.sleep(preExecutionWaitInMillis);
                    WORKERS_SLEEPING.decrementAndGet();
                    LOG.debug("Worker {} woke up to execute {}", id, transactionType.getName());
                } catch (InterruptedException e) {
                    LOG.error("Worker {} pre-execution sleep interrupted", id, e);
                }
            }

            WORKERS_WORKING.incrementAndGet();
            long start = System.nanoTime();

            TransactionStatus status = doWork(configuration.getDatabaseType(), transactionType);

            long end = System.nanoTime();
            WORKERS_WORKING.decrementAndGet();

            TRANSACTIONS.tag("type", "any").register(Metrics.globalRegistry).increment();
            TRANSACTIONS.tag("type", transactionType.getName()).register(Metrics.globalRegistry).increment();
            TRANSACTION_DURATION.tag("type", "any").register(Metrics.globalRegistry)
                    .record(Duration.ofNanos(end - start));
            TRANSACTION_DURATION.tag("type", transactionType.getName()).register(Metrics.globalRegistry)
                    .record(Duration.ofNanos(end - start));

            if (benchmarkState.isMeasuring()) {
                boolean isSuccess = status == TransactionStatus.SUCCESS ||
                    status == TransactionStatus.USER_ABORTED;
                resultStats.addLatency(
                    transactionType.getId(),
                    start,
                    end,
                    isSuccess);
                intervalRequests.incrementAndGet();
            }

            long postExecutionWaitInMillis = getPostExecutionWaitInMillis(transactionType);
            if (postExecutionWaitInMillis > 0) {
                try {
                    LOG.debug("Worker {} {} will sleep for {} ms after executing",
                        id, transactionType.getName(), postExecutionWaitInMillis);

                    WORKERS_SLEEPING.incrementAndGet();
                    Thread.sleep(postExecutionWaitInMillis);
                    WORKERS_SLEEPING.decrementAndGet();
                } catch (InterruptedException e) {
                    LOG.error("Worker {} post-execution sleep interrupted", id, e);
                }
            }
        }

        LOG.debug("Worker {} calling teardown", id);

        tearDown();
        benchmarkState.workerFinished();
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

            while (retryCount <= maxRetryCount && this.benchmarkState.isWorkingOrMeasuring()) {
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

                        CONNECTION_ERRORS.tag("type", "any")
                                .register(Metrics.globalRegistry).increment();
                        CONNECTION_ERRORS.tag("type", String.valueOf(ex.getErrorCode()))
                                .register(Metrics.globalRegistry).increment();

                        continue;
                    }
                }

                long start = System.nanoTime();
                try {
                    LOG.debug("Worker {} is attempting {}", id, transactionType);

                    status = this.executeWork(conn, transactionType);

                    LOG.debug("Worker {} completed {} with status {} and going to commit",
                        id, transactionType, status.name());

                    conn.commit();
                    break;
                } catch (UserAbortException ex) {
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

                } catch (SQLException | UnexpectedResultException ex) {
                    Status error = Status.of(StatusCode.CLIENT_INTERNAL_ERROR);
                    int errorCode = error.getCode().getCode();
                    boolean isRetryable  = false;
                    if  (ex instanceof SQLException) {
                        errorCode = ((SQLException)ex).getErrorCode();
                        isRetryable = ex instanceof SQLRecoverableException || ex instanceof SQLTransientException;
                        if (ex instanceof YdbStatusable) {
                            error = ((YdbStatusable)ex).getStatus();
                        }
                    }
                    if (ex instanceof UnexpectedResultException) {
                        error = ((UnexpectedResultException) ex).getStatus();
                        errorCode = error.getCode().getCode();
                        isRetryable = error.getCode().isRetryable(true);
                    }

                    YDB_ERRORS.tag("type", "any").register(Metrics.globalRegistry).increment();
                    YDB_ERRORS.tag("type", error.getCode().toString()).register(Metrics.globalRegistry).increment();

                    try {
                        LOG.debug("Worker {} rolled back transaction {}", id, transactionType);
                        conn.rollback();
                    } catch (Exception e) {
                        LOG.warn("Failed to rollback transaction", e);
                    }

                    if (isRetryable) {
                        LOG.debug(
                            "Worker {} Retryable SQLException occurred during [{}]... current retry attempt [{}], max retry attempts [{}], error code [{}].",
                            id, transactionType, retryCount, maxRetryCount, errorCode, ex);

                        retryCount++;

                        if (retryCount <= maxRetryCount) {
                            status = TransactionStatus.RETRY;
                            long sleepMs = backoffTimeMillis(error.getCode(), retryCount);
                            try {
                                Thread.sleep(sleepMs);
                            } catch (InterruptedException e) {
                                LOG.error("Retry sleep interrupted", e);
                            }
                        } else {
                            status = TransactionStatus.ERROR;
                            LOG.warn(
                                "Worker {} Retryable SQLException occurred during [{}]... current retry attempt [{}], max retry attempts [{}], error code [{}].",
                                id, transactionType, retryCount, maxRetryCount, errorCode, ex);
                        }
                    } else {
                        LOG.warn(
                            "Worker {} SQLException occurred during [{}] and will not be retried..., error code [{}].",
                            id, transactionType, errorCode, ex);

                        status = TransactionStatus.ERROR;

                        break;
                    }
                } finally {
                    if (this.configuration.getNewConnectionPerTxn() && this.conn != null) {
                        try {
                            LOG.debug("Worker {} closing connection", id);
                            this.conn.close();
                            this.conn = null;
                        } catch (SQLException e) {
                            LOG.error("Worker {} connection couldn't be closed.", id, e);
                            throw new RuntimeException("Failed to close connection", e);
                        }
                    }

                    long end = System.nanoTime();

                    EXECUTION_DURATION.tag("type", "any").register(Metrics.globalRegistry)
                            .record(Duration.ofNanos(end - start));
                    EXECUTION_DURATION.tag("type", status.toString()).register(Metrics.globalRegistry)
                            .record(Duration.ofNanos(end - start));

                    EXECUTIONS.tag("type", "any").register(Metrics.globalRegistry).increment();
                    EXECUTIONS.tag("type", status.toString()).register(Metrics.globalRegistry).increment();

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
        } catch (Exception ex) {
            String msg = String.format("Unexpected Exception in '%s' when executing '%s' on [%s]", this, transactionType, databaseType.name());
            LOG.error(msg);
            throw ex;
        }

        return status;
    }

    private long backoffTimeMillisInternal(int retryNumber, long backoffSlotMillis, int backoffCeiling) {
        int slots = 1 << Math.min(retryNumber - 1, backoffCeiling);
        long delay = backoffSlotMillis * slots;
        return delay + ThreadLocalRandom.current().nextLong(delay);
    }

    private long slowBackoffTimeMillis(int retryNumber) {
        return backoffTimeMillisInternal(
            retryNumber,
            configuration.getBackoffSlotMillis(),
            configuration.getBackoffCeiling());
    }

    private long fastBackoffTimeMillis(int retryNumber) {
        return backoffTimeMillisInternal(
            retryNumber,
            configuration.getFastBackoffSlotMillis(),
            configuration.getFastBackoffCeiling());
    }

    private long backoffTimeMillis(StatusCode code, int retryNumber) {
        switch (code) {
            case BAD_SESSION:
                // Instant retry
                return 0;
            case ABORTED:
            case CLIENT_CANCELLED:
            case CLIENT_INTERNAL_ERROR:
            case SESSION_BUSY:
            case TRANSPORT_UNAVAILABLE:
            case UNAVAILABLE:
            case UNDETERMINED:
                // Fast backoff
                return fastBackoffTimeMillis(retryNumber);
            case NOT_FOUND:
            case OVERLOADED:
            case CLIENT_RESOURCE_EXHAUSTED:
            default:
                // Slow backoff
                return slowBackoffTimeMillis(retryNumber);
        }
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
            } catch (SQLException e) {
                LOG.error("Worker {} connection couldn't be closed.", id, e);
            }
        }
    }

    protected long getPreExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

    protected long getPostExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

}
