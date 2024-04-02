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

import tech.ydb.jdbc.YdbConnection;

import tech.ydb.core.UnexpectedResultException;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;

import java.time.Duration;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {

    private static final int BACKOFF_MILLIS = 10;
    private static final int BACKOFF_CEILING = 5;
    private static final int MAX_RETRIES = 100;

    public final class YDBConnectionHelper {
        private final YdbConnection ydbConn;
        private final TableClient tableClient;
        private final SessionRetryContext retryCtx;

        public YDBConnectionHelper(Connection conn) {
            try {
                this.ydbConn = conn.unwrap(YdbConnection.class);
                this.tableClient = ydbConn.getCtx().getTableClient();
                this.retryCtx = SessionRetryContext.create(tableClient)
                    .maxRetries(MAX_RETRIES)
                    .backoffCeiling(BACKOFF_CEILING)
                    .backoffSlot(Duration.ofMillis(BACKOFF_MILLIS))
                    .build();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        String getDatabase() {
            return this.ydbConn.getCtx().getDatabase();
        }
    }

    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    private final long numWarehouses;

    public TPCCLoader(TPCCBenchmark benchmark) {
        super(benchmark);
        numWarehouses = Math.max(Math.round(TPCCConfig.configWhseCount * this.scaleFactor), 1);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int maxConcurrent = workConf.getLoaderThreads();

        final int firstWarehouse = this.startFromId;
        final int lastWarehouse = this.startFromId + (int)numWarehouses - 1;
        final int lastWarehouseInCompany = this.startFromId + this.totalWarehousesInCompany - 1;

        // ITEM, WAREHOUSE, DISTRICT
        if (firstWarehouse == 1) {
            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);
                    loadItems(ydbConnHelper, TPCCConfig.configItemCount);
                    loadWarehouses(ydbConnHelper, firstWarehouse, lastWarehouseInCompany);
                    loadDistricts(ydbConnHelper, firstWarehouse, lastWarehouseInCompany, TPCCConfig.configDistPerWhse);
                }
            });
        }

        int warehousesPerThread = Math.max((int)numWarehouses / maxConcurrent, 1);
        for (int w = this.startFromId; w <= lastWarehouse; w += warehousesPerThread) {
            final int loadFrom = w;
            final int loadUntil = Math.min(loadFrom + warehousesPerThread - 1, lastWarehouse);
            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load stock warehouses from {} to {}", loadFrom, loadUntil);

                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);
                    loadStock(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configItemCount);
                }
            };
            threads.add(t);

            t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load customers warehouses from {} to {}", loadFrom, loadUntil);

                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);
                    loadCustomers(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);

            t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load order_line and new orders, warehouses from {} to {}", loadFrom, loadUntil);

                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);
                    loadNewOrders(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                    loadOrderLines(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);

            t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    LOG.debug("Starting to load history and open orders, warehouses from {} to {}", loadFrom, loadUntil);

                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);

                    loadCustomerHistory(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                    loadOpenOrders(ydbConnHelper, loadFrom, loadUntil, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);

        }
        return (threads);
    }

    protected void loadItems(YDBConnectionHelper ydbConnHelper, int itemCount) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("I_ID", PrimitiveType.Int32);
        ydbTypes.put("I_NAME", PrimitiveType.Text);
        ydbTypes.put("I_PRICE", PrimitiveType.Double);
        ydbTypes.put("I_DATA", PrimitiveType.Text);
        ydbTypes.put("I_IM_ID", PrimitiveType.Int32);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {

            for (int i = 1; i <= itemCount; i++) {
                Map<String, Value<?>> rowValues = new HashMap<>();
                rowValues.put("I_ID", PrimitiveValue.newInt32(i));
                rowValues.put("I_NAME", PrimitiveValue.newText(
                    TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()))));
                rowValues.put("I_PRICE", PrimitiveValue.newDouble(
                    TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0));


                Item item = new Item();
                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
                item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                // i_data
                int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    rowValues.put("I_DATA", PrimitiveValue.newText(TPCCUtil.randomStr(len)));
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    rowValues.put("I_DATA", PrimitiveValue.newText(
                        TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9)));
                }
                rowValues.put("I_IM_ID", PrimitiveValue.newInt32(TPCCUtil.randomNumber(1, 10000, benchmark.rng())));

                batch.add(structType.newValue(rowValues));
                if (batch.size() == workConf.getBatchSize()) {
                    executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_ITEM, structType, batch);
                }

            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_ITEM, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    protected void loadWarehouses(YDBConnectionHelper ydbConnHelper, int start_id, int last_id) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("W_ID", PrimitiveType.Int32);
        ydbTypes.put("W_YTD", PrimitiveType.Double);
        ydbTypes.put("W_TAX", PrimitiveType.Double);
        ydbTypes.put("W_NAME", PrimitiveType.Text);
        ydbTypes.put("W_STREET_1", PrimitiveType.Text);
        ydbTypes.put("W_STREET_2", PrimitiveType.Text);
        ydbTypes.put("W_CITY", PrimitiveType.Text);
        ydbTypes.put("W_STATE", PrimitiveType.Text);
        ydbTypes.put("W_ZIP", PrimitiveType.Text);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; ++w_id) {
                Map<String, Value<?>> rowValues = new HashMap<>();
                rowValues.put("W_ID", PrimitiveValue.newInt32(w_id));
                rowValues.put("W_YTD", PrimitiveValue.newDouble(300000));
                rowValues.put("W_TAX", PrimitiveValue.newDouble(
                    (TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0));
                rowValues.put("W_NAME", PrimitiveValue.newText(
                    TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()))));
                rowValues.put("W_STREET_1", PrimitiveValue.newText(
                    TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                rowValues.put("W_STREET_2", PrimitiveValue.newText(
                    TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                rowValues.put("W_CITY", PrimitiveValue.newText(
                    TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                rowValues.put("W_STATE", PrimitiveValue.newText(TPCCUtil.randomStr(3).toUpperCase()));
                rowValues.put("W_ZIP", PrimitiveValue.newText("123456789"));

                batch.add(structType.newValue(rowValues));

                if (batch.size() == workConf.getBatchSize()) {
                    executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_WAREHOUSE, structType, batch);
                }
            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_WAREHOUSE, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    protected void loadStock(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int numItems) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("S_W_ID", PrimitiveType.Int32);
        ydbTypes.put("S_I_ID", PrimitiveType.Int32);
        ydbTypes.put("S_QUANTITY", PrimitiveType.Int32);
        ydbTypes.put("S_ORDER_CNT", PrimitiveType.Int32);
        ydbTypes.put("S_REMOTE_CNT", PrimitiveType.Int32);
        ydbTypes.put("S_DATA", PrimitiveType.Text);
        ydbTypes.put("S_DIST_01", PrimitiveType.Text);
        ydbTypes.put("S_DIST_02", PrimitiveType.Text);
        ydbTypes.put("S_DIST_03", PrimitiveType.Text);
        ydbTypes.put("S_DIST_04", PrimitiveType.Text);
        ydbTypes.put("S_DIST_05", PrimitiveType.Text);
        ydbTypes.put("S_DIST_06", PrimitiveType.Text);
        ydbTypes.put("S_DIST_07", PrimitiveType.Text);
        ydbTypes.put("S_DIST_08", PrimitiveType.Text);
        ydbTypes.put("S_DIST_09", PrimitiveType.Text);
        ydbTypes.put("S_DIST_10", PrimitiveType.Text);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {

                for (int i = 1; i <= numItems; i++) {
                    Map<String, Value<?>> rowValues = new HashMap<>();
                    rowValues.put("S_W_ID", PrimitiveValue.newInt32(w_id));
                    rowValues.put("S_I_ID", PrimitiveValue.newInt32(i));
                    rowValues.put("S_QUANTITY", PrimitiveValue.newInt32(TPCCUtil.randomNumber(10, 100, benchmark.rng())));
                    rowValues.put("S_ORDER_CNT", PrimitiveValue.newInt32(0));
                    rowValues.put("S_REMOTE_CNT", PrimitiveValue.newInt32(0));

                    // s_data
                    int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                    int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                    if (randPct > 10) {
                        // 90% of time i_data isa random string of length [26 ..
                        // 50]
                        rowValues.put("S_DATA", PrimitiveValue.newText(TPCCUtil.randomStr(len)));
                    } else {
                        // 10% of time i_data has "ORIGINAL" crammed somewhere
                        // in middle
                        int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                        String data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                        rowValues.put("S_DATA", PrimitiveValue.newText(data));
                    }

                    rowValues.put("S_DIST_01", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_02", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_03", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_04", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_05", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_06", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_07", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_08", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_09", PrimitiveValue.newText(TPCCUtil.randomStr(24)));
                    rowValues.put("S_DIST_10", PrimitiveValue.newText(TPCCUtil.randomStr(24)));

                    batch.add(structType.newValue(rowValues));
                    if (batch.size() == workConf.getBatchSize()) {
                        executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_STOCK, structType, batch);
                    }

                }

            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_STOCK, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadDistricts(YDBConnectionHelper ydbConnHelper,
                                 int start_id, int last_id, int districtsPerWarehouse) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("D_W_ID", PrimitiveType.Int32);
        ydbTypes.put("D_ID", PrimitiveType.Int32);
        ydbTypes.put("D_YTD", PrimitiveType.Double);
        ydbTypes.put("D_TAX", PrimitiveType.Double);
        ydbTypes.put("D_NEXT_O_ID", PrimitiveType.Int32);
        ydbTypes.put("D_NAME", PrimitiveType.Text);
        ydbTypes.put("D_STREET_1", PrimitiveType.Text);
        ydbTypes.put("D_STREET_2", PrimitiveType.Text);
        ydbTypes.put("D_CITY", PrimitiveType.Text);
        ydbTypes.put("D_STATE", PrimitiveType.Text);
        ydbTypes.put("D_ZIP", PrimitiveType.Text);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    Map<String, Value<?>> rowValues = new HashMap<>();
                    rowValues.put("D_W_ID", PrimitiveValue.newInt32(w_id));
                    rowValues.put("D_ID", PrimitiveValue.newInt32(d));
                    rowValues.put("D_YTD", PrimitiveValue.newDouble(30000));
                    rowValues.put("D_TAX", PrimitiveValue.newDouble(
                        (double) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0)));
                    rowValues.put("D_NEXT_O_ID", PrimitiveValue.newInt32(TPCCConfig.configCustPerDist + 1));
                    rowValues.put("D_NAME", PrimitiveValue.newText(
                        TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()))));
                    rowValues.put("D_STREET_1", PrimitiveValue.newText(
                        TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                    rowValues.put("D_STREET_2", PrimitiveValue.newText(
                        TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                    rowValues.put("D_CITY", PrimitiveValue.newText(
                        TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                    rowValues.put("D_STATE", PrimitiveValue.newText(
                        TPCCUtil.randomStr(3).toUpperCase()));
                    rowValues.put("D_ZIP", PrimitiveValue.newText("123456789"));

                    batch.add(structType.newValue(rowValues));
                    if (batch.size() == workConf.getBatchSize()) {
                        executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_DISTRICT, structType, batch);
                    }
                }
            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_DISTRICT, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadCustomers(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("C_W_ID", PrimitiveType.Int32);
        ydbTypes.put("C_D_ID", PrimitiveType.Int32);
        ydbTypes.put("C_ID", PrimitiveType.Int32);
        ydbTypes.put("C_DISCOUNT", PrimitiveType.Double);
        ydbTypes.put("C_CREDIT", PrimitiveType.Text);
        ydbTypes.put("C_LAST", PrimitiveType.Text);
        ydbTypes.put("C_FIRST", PrimitiveType.Text);
        ydbTypes.put("C_CREDIT_LIM", PrimitiveType.Double);
        ydbTypes.put("C_BALANCE", PrimitiveType.Double);
        ydbTypes.put("C_YTD_PAYMENT", PrimitiveType.Double);
        ydbTypes.put("C_PAYMENT_CNT", PrimitiveType.Int32);
        ydbTypes.put("C_DELIVERY_CNT", PrimitiveType.Int32);
        ydbTypes.put("C_STREET_1", PrimitiveType.Text);
        ydbTypes.put("C_STREET_2", PrimitiveType.Text);
        ydbTypes.put("C_CITY", PrimitiveType.Text);
        ydbTypes.put("C_STATE", PrimitiveType.Text);
        ydbTypes.put("C_ZIP", PrimitiveType.Text);
        ydbTypes.put("C_PHONE", PrimitiveType.Text);
        ydbTypes.put("C_SINCE", PrimitiveType.Timestamp);
        ydbTypes.put("C_MIDDLE", PrimitiveType.Text);
        ydbTypes.put("C_DATA", PrimitiveType.Text);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    for (int c = 1; c <= customersPerDistrict; c++) {
                        Map<String, Value<?>> rowValues = new HashMap<>();
                        rowValues.put("C_W_ID", PrimitiveValue.newInt32(w_id));
                        rowValues.put("C_D_ID", PrimitiveValue.newInt32(d));
                        rowValues.put("C_ID", PrimitiveValue.newInt32(c));
                        rowValues.put("C_DISCOUNT", PrimitiveValue.newDouble(
                            (double) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0)));

                        if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
                            rowValues.put("C_CREDIT", PrimitiveValue.newText("BC")); // 10% Bad Credit
                        } else {
                            rowValues.put("C_CREDIT", PrimitiveValue.newText("GC")); // 90% Good Credit
                        }
                        if (c <= 1000) {
                            rowValues.put("C_LAST", PrimitiveValue.newText(TPCCUtil.getLastName(c - 1)));
                        } else {
                            rowValues.put("C_LAST", PrimitiveValue.newText(
                                TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng())));
                        }

                        rowValues.put("C_FIRST", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()))));
                        rowValues.put("C_CREDIT_LIM", PrimitiveValue.newDouble(50000));
                        rowValues.put("C_BALANCE", PrimitiveValue.newDouble(-10));
                        rowValues.put("C_YTD_PAYMENT", PrimitiveValue.newDouble(10));
                        rowValues.put("C_PAYMENT_CNT", PrimitiveValue.newInt32(1));
                        rowValues.put("C_DELIVERY_CNT", PrimitiveValue.newInt32(0));
                        rowValues.put("C_STREET_1", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                        rowValues.put("C_STREET_2", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                        rowValues.put("C_CITY", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()))));
                        rowValues.put("C_STATE", PrimitiveValue.newText(TPCCUtil.randomStr(3).toUpperCase()));

                        // TPC-C 4.3.2.7: 4 random digits + "11111"
                        rowValues.put("C_ZIP", PrimitiveValue.newText(TPCCUtil.randomNStr(4) + "11111"));
                        rowValues.put("C_PHONE", PrimitiveValue.newText(TPCCUtil.randomNStr(16)));
                        rowValues.put("C_SINCE", PrimitiveValue.newTimestamp(System.currentTimeMillis() * 1000));
                        rowValues.put("C_MIDDLE", PrimitiveValue.newText("OE"));
                        rowValues.put("C_DATA", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()))));

                        batch.add(structType.newValue(rowValues));
                        if (batch.size() == workConf.getBatchSize()) {
                            executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_CUSTOMER, structType, batch);
                        }
                    }
                }
            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_CUSTOMER, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    protected void loadCustomerHistory(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("H_C_ID", PrimitiveType.Int32);
        ydbTypes.put("H_C_D_ID", PrimitiveType.Int32);
        ydbTypes.put("H_C_W_ID", PrimitiveType.Int32);
        ydbTypes.put("H_D_ID", PrimitiveType.Int32);
        ydbTypes.put("H_W_ID", PrimitiveType.Int32);
        ydbTypes.put("H_DATE", PrimitiveType.Timestamp);
        ydbTypes.put("H_AMOUNT", PrimitiveType.Double);
        ydbTypes.put("H_DATA", PrimitiveType.Text);
        ydbTypes.put("H_C_NANO_TS", PrimitiveType.Int64);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            long prevTs = 0;
            for (int w_id = start_id; w_id <= last_id; w_id++) {
                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    for (int c = 1; c <= customersPerDistrict; c++) {
                        long millis = System.currentTimeMillis();
                        long ts = System.nanoTime();
                        if (ts <= prevTs) {
                            ts = prevTs + 1;
                        }

                        Map<String, Value<?>> rowValues = new HashMap<>();
                        rowValues.put("H_C_ID", PrimitiveValue.newInt32(c));
                        rowValues.put("H_C_D_ID", PrimitiveValue.newInt32(d));
                        rowValues.put("H_C_W_ID", PrimitiveValue.newInt32(w_id));
                        rowValues.put("H_D_ID", PrimitiveValue.newInt32(d));
                        rowValues.put("H_W_ID", PrimitiveValue.newInt32(w_id));
                        rowValues.put("H_DATE", PrimitiveValue.newTimestamp(millis*1000));
                        rowValues.put("H_AMOUNT", PrimitiveValue.newDouble(10));
                        rowValues.put("H_DATA", PrimitiveValue.newText(
                            TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()))));
                        rowValues.put("H_C_NANO_TS", PrimitiveValue.newInt64(ts));

                        prevTs = ts;

                        batch.add(structType.newValue(rowValues));
                        if (batch.size() == workConf.getBatchSize()) {
                            executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_HISTORY, structType, batch);
                        }
                    }
                }
            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_HISTORY, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadOpenOrders(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("O_W_ID", PrimitiveType.Int32);
        ydbTypes.put("O_D_ID", PrimitiveType.Int32);
        ydbTypes.put("O_ID", PrimitiveType.Int32);
        ydbTypes.put("O_C_ID", PrimitiveType.Int32);
        ydbTypes.put("O_CARRIER_ID", PrimitiveType.Int32);
        ydbTypes.put("O_OL_CNT", PrimitiveType.Int32);
        ydbTypes.put("O_ALL_LOCAL", PrimitiveType.Int32);
        ydbTypes.put("O_ENTRY_D", PrimitiveType.Timestamp);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {

                for (int d = 1; d <= districtsPerWarehouse; d++) {
                    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                    int[] c_ids = new int[customersPerDistrict];
                    for (int i = 0; i < customersPerDistrict; ++i) {
                        c_ids[i] = i + 1;
                    }
                    // Collections.shuffle exists, but there is no
                    // Arrays.shuffle
                    for (int i = 0; i < c_ids.length - 1; ++i) {
                        int remaining = c_ids.length - i - 1;
                        int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

                        int temp = c_ids[swapIndex];
                        c_ids[swapIndex] = c_ids[i];
                        c_ids[i] = temp;
                    }

                    for (int c = 1; c <= customersPerDistrict; c++) {
                        Map<String, Value<?>> rowValues = new HashMap<>();
                        rowValues.put("O_W_ID", PrimitiveValue.newInt32(w_id));
                        rowValues.put("O_D_ID", PrimitiveValue.newInt32(d));
                        rowValues.put("O_ID", PrimitiveValue.newInt32(c));
                        rowValues.put("O_C_ID", PrimitiveValue.newInt32(c_ids[c - 1]));

                        // o_carrier_id is set *only* for orders with ids < 2101
                        // [4.3.3.1]
                        if (c < FIRST_UNPROCESSED_O_ID) {
                            rowValues.put("O_CARRIER_ID", PrimitiveValue.newInt32(
                                TPCCUtil.randomNumber(1, 10, benchmark.rng())));
                        } else {
                            rowValues.put("O_CARRIER_ID", PrimitiveValue.newInt32(0));
                        }

                        rowValues.put("O_OL_CNT", PrimitiveValue.newInt32(getRandomCount(w_id, c, d)));
                        rowValues.put("O_ALL_LOCAL", PrimitiveValue.newInt32(1));
                        rowValues.put("O_ENTRY_D", PrimitiveValue.newTimestamp(System.currentTimeMillis()*1000));

                        batch.add(structType.newValue(rowValues));
                        if (batch.size() == workConf.getBatchSize()) {
                            executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_OPENORDER, structType, batch);
                        }

                    }

                }

            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_OPENORDER, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    private int getRandomCount(int w_id, int c, int d) {
        Customer customer = new Customer();
        customer.c_id = c;
        customer.c_d_id = d;
        customer.c_w_id = w_id;

        Random random = new Random(customer.hashCode());

        return TPCCUtil.randomNumber(5, 15, random);
    }

    protected void loadNewOrders(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("NO_W_ID", PrimitiveType.Int32);
        ydbTypes.put("NO_D_ID", PrimitiveType.Int32);
        ydbTypes.put("NO_O_ID", PrimitiveType.Int32);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {

                for (int d = 1; d <= districtsPerWarehouse; d++) {

                    for (int c = 1; c <= customersPerDistrict; c++) {

                        // 900 rows in the NEW-ORDER table corresponding to the last
                        // 900 rows in the ORDER table for that district (i.e.,
                        // with NO_O_ID between 2,101 and 3,000)
                        if (c >= FIRST_UNPROCESSED_O_ID) {
                            Map<String, Value<?>> rowValues = new HashMap<>();
                            rowValues.put("NO_W_ID", PrimitiveValue.newInt32(w_id));
                            rowValues.put("NO_D_ID", PrimitiveValue.newInt32(d));
                            rowValues.put("NO_O_ID", PrimitiveValue.newInt32(c));
                            batch.add(structType.newValue(rowValues));
                        }

                        if (batch.size() == workConf.getBatchSize()) {
                            executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_NEWORDER, structType, batch);
                        }

                    }

                }

            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_NEWORDER, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadOrderLines(YDBConnectionHelper ydbConnHelper, int start_id, int last_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("OL_W_ID", PrimitiveType.Int32);
        ydbTypes.put("OL_D_ID", PrimitiveType.Int32);
        ydbTypes.put("OL_O_ID", PrimitiveType.Int32);
        ydbTypes.put("OL_NUMBER", PrimitiveType.Int32);
        ydbTypes.put("OL_I_ID", PrimitiveType.Int32);
        ydbTypes.put("OL_DELIVERY_D", PrimitiveType.Timestamp);
        ydbTypes.put("OL_AMOUNT", PrimitiveType.Double);
        ydbTypes.put("OL_SUPPLY_W_ID", PrimitiveType.Int32);
        ydbTypes.put("OL_QUANTITY", PrimitiveType.Double);
        ydbTypes.put("OL_DIST_INFO", PrimitiveType.Text);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {
            for (int w_id = start_id; w_id <= last_id; w_id++) {

                for (int d = 1; d <= districtsPerWarehouse; d++) {

                    for (int c = 1; c <= customersPerDistrict; c++) {

                        int count = getRandomCount(w_id, c, d);

                        for (int l = 1; l <= count; l++) {

                            Map<String, Value<?>> rowValues = new HashMap<>();
                            rowValues.put("OL_W_ID", PrimitiveValue.newInt32(w_id));
                            rowValues.put("OL_D_ID", PrimitiveValue.newInt32(d));
                            rowValues.put("OL_O_ID", PrimitiveValue.newInt32(c));
                            rowValues.put("OL_NUMBER", PrimitiveValue.newInt32(l));

                            int ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
                            rowValues.put("OL_I_ID", PrimitiveValue.newInt32(ol_i_id));

                            if (ol_i_id < FIRST_UNPROCESSED_O_ID) {
                                rowValues.put("OL_DELIVERY_D", PrimitiveValue.newTimestamp(System.currentTimeMillis()*1000));
                                rowValues.put("OL_AMOUNT", PrimitiveValue.newDouble(0));
                            } else {
                                rowValues.put("OL_DELIVERY_D", PrimitiveValue.newTimestamp(0));
                                // random within [0.01 .. 9,999.99]
                                double amount = (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
                                rowValues.put("OL_AMOUNT", PrimitiveValue.newDouble(amount));
                            }

                            rowValues.put("OL_SUPPLY_W_ID", PrimitiveValue.newInt32(w_id));
                            rowValues.put("OL_QUANTITY", PrimitiveValue.newDouble(5));
                            rowValues.put("OL_DIST_INFO", PrimitiveValue.newText(TPCCUtil.randomStr(24)));

                            batch.add(structType.newValue(rowValues));

                            if (batch.size() == workConf.getBatchSize()) {
                                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_ORDERLINE, structType, batch);
                            }

                        }

                    }

                }

            }

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_ORDERLINE, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void executeWithRetry(Runnable execution) throws SQLException {
        final int maxRetries = MAX_RETRIES;
        final int waitTimeCeilingMs = (1 << BACKOFF_CEILING) * BACKOFF_MILLIS;

        int retryCount = 0;
        int waitTimeMs = BACKOFF_MILLIS;
        while (retryCount < maxRetries) {
            try {
                execution.run();
                break;
            } catch (Exception ex) {
                if (retryCount >= maxRetries) {
                    LOG.error("Retries executing batch exceeded: " + ex.getMessage());
                    throw ex;
                }
                try {
                    Thread.sleep(waitTimeMs + benchmark.rng().nextInt(100));
                } catch (InterruptedException e) {
                    String s = String.format("Interrupted during retry: %s, original exception: %s",
                        e.toString(), ex.toString());
                    throw new RuntimeException(s, ex);
                }
                waitTimeMs = Math.min(waitTimeMs * 2, waitTimeCeilingMs);
                retryCount++;
                LOG.debug(String.format("Retrying %d time batch execution after retryable exception: %s",
                    retryCount, ex.getMessage()));
            }
        }
    }

    protected void loadItems(Connection conn, int itemCount) {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Utf8,p3:Double,p4:Utf8,p5:Int32>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as I_ID, $row.p2 as I_NAME, " +
            "$row.p3 as I_PRICE, $row.p4 as I_DATA, $row.p5 as I_IM_ID));\n" +
            "upsert into " + TPCCConstants.TABLENAME_ITEM + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement itemPrepStmt = conn.prepareStatement(sql)) {

            int batchSize = 0;
            for (int i = 1; i <= itemCount; i++) {

                Item item = new Item();
                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
                item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                // i_data
                int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

                int idx = 1;
                itemPrepStmt.setInt(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setInt(idx, item.i_im_id);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getBatchSize()) {
                    executeBatchWithRetry(itemPrepStmt);
                    batchSize = 0;
                }
            }


            if (batchSize > 0) {
                executeBatchWithRetry(itemPrepStmt);
            }

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void executeBulkUpsert(YDBConnectionHelper ydbConnHelper, String tableName, StructType structType, List<Value<?>> batch) throws SQLException {
        if (batch.size() == 0) {
            return;
        }

        ListValue bulkData = ListType.of(structType).newValue(batch);
        String path = String.format("%s/%s", ydbConnHelper.getDatabase(), tableName);
        batch.clear();

        // not all errors are retried by SDK, for example CLIENT_DEADLINE_EXPIRED.
        // but we want to retry it here.
        executeWithRetry(() -> {
            try {
                CompletableFuture<tech.ydb.core.Status> future =
                    ydbConnHelper.retryCtx.supplyStatus(session -> session.executeBulkUpsert(path, bulkData));

                future.join().expectSuccess(String.format("bulk upsert problem, table: %s", tableName));
            } catch (UnexpectedResultException e) {
                LOG.debug(String.format("Error executing bulk upsert, table: %s, exception: %s", tableName, e.getMessage()));
                throw new RuntimeException(e);
            }
        });
    }

    protected void executeBatchWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.executeBatch();
                statement.clearBatch();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    protected void executeWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.execute();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    protected void executeUpdateWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
