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


package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TPCCBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(TPCCBenchmark.class);

    public TPCCBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (NewOrder.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

        try {
            List<TPCCWorker> terminals = createTerminals();
            workers.addAll(terminals);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return workers;
    }

    @Override
    protected Loader<TPCCBenchmark> makeLoaderImpl() {
        return new TPCCLoader(this);
    }

    protected List<TPCCWorker> createTerminals() throws SQLException {

        TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();
        if (numWarehouses <= 0) {
            numWarehouses = 1;
        }

        final int startWarehouseId = workConf.getStartFromId();

        final int numTerminals = workConf.getTerminals();

        final int terminalsPerWarehouse = (int) numTerminals / numWarehouses;

        assert (terminalsPerWarehouse == 10); // according TPC-C

        final int lowerDistrictId = 1;
        final int upperDistrictId = terminalsPerWarehouse;

        int terminalIndex = 0;
        int workerId = startWarehouseId / numWarehouses * numTerminals;

        for (int w = startWarehouseId - 1; w < numWarehouses + startWarehouseId - 1; w++) {
            final int w_id = w + 1;

            for (int terminalId = 0; terminalId < terminalsPerWarehouse; terminalId++) {
                TPCCWorker terminal = new TPCCWorker(this, workerId++, w_id, lowerDistrictId, upperDistrictId, numWarehouses);
                terminals[terminalIndex++] = terminal;
            }
        }

        return Arrays.asList(terminals);
    }
}
