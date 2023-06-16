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

import tech.ydb.jdbc.connection.YdbContext;
import tech.ydb.jdbc.exception.YdbConditionallyRetryableException;
import tech.ydb.jdbc.exception.YdbRetryableException;
import tech.ydb.jdbc.YdbConnection;

import tech.ydb.core.Result;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TxControl;

import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.ThreadLocalRandomGenerator;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {

    private class YDBConnectionHelper {
        private final YdbConnection ydbConn;
        private final TableClient tableClient;
        private final SessionRetryContext retryCtx;

        public YDBConnectionHelper(Connection conn) {
            try {
                this.ydbConn = conn.unwrap(YdbConnection.class);
                this.tableClient = ydbConn.getCtx().getTableClient();
                this.retryCtx = SessionRetryContext.create(tableClient)
                    .maxRetries(20)
                    .backoffCeiling(7)
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
        final CountDownLatch itemLatch = new CountDownLatch(1);
        final Boolean noBulkUpload = this.noBulkUpload;

        // ITEM
        // This will be invoked first and executed in a single thread.
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                if (noBulkUpload) {
                    loadItems(conn, TPCCConfig.configItemCount);
                } else {
                    YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);
                    loadItems(ydbConnHelper, TPCCConfig.configItemCount);
                }
            }

            @Override
            public void afterLoad() {
                itemLatch.countDown();
            }
        });

        // We assume that partitioning is the same as set by table creation. It would be better to
        // describe the tables and get boundaries.
        //
        // Note that if warehouses are split between many shards, then we want to
        // start in parallel threads, which write to different shards.
        // I.e. don't start in parallel warehouses 1 and 2, which are on the same shard,
        // but start 1,2 - thread1 and 100,101 - thread2
        final int lastWarehouse = this.startFromId + (int)numWarehouses - 1;
        for (int w = this.startFromId; w <= lastWarehouse; w += this.warehousesPerShard) {
            final int loadFrom = w;
            final int loadUntil = Math.min(loadFrom + this.warehousesPerShard - 1, lastWarehouse);
            LoaderThread t = new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) {
                    for (int w_id = loadFrom; w_id <= loadUntil; ++w_id) {
                        if (noBulkUpload) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load WAREHOUSE {}", w_id);
                            }
                            // WAREHOUSE
                            loadWarehouse(conn, w_id);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load STOCK {}", w_id);
                            }
                            // STOCK
                            loadStock(conn, w_id, TPCCConfig.configItemCount);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load DISTRICT {}", w_id);
                            }
                            // DISTRICT
                            loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load CUSTOMER {}", w_id);
                            }
                            // CUSTOMER
                            loadCustomers(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load CUSTOMER HISTORY {}", w_id);
                            }
                            // CUSTOMER HISTORY
                            loadCustomerHistory(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load ORDERS {}", w_id);
                            }
                            // ORDERS
                            loadOpenOrders(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load NEW ORDERS {}", w_id);
                            }
                            // NEW ORDERS
                            loadNewOrders(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load ORDER LINES {}", w_id);
                            }
                            // ORDER LINES
                            loadOrderLines(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                        } else {
                            YDBConnectionHelper ydbConnHelper = new YDBConnectionHelper(conn);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load WAREHOUSE {}", w_id);
                            }
                            // WAREHOUSE
                            loadWarehouse(conn, w_id);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load STOCK {}", w_id);
                            }
                            // STOCK
                            loadStock(ydbConnHelper, w_id, TPCCConfig.configItemCount);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load DISTRICT {}", w_id);
                            }
                            // DISTRICT
                            loadDistricts(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load CUSTOMER {}", w_id);
                            }
                            // CUSTOMER
                            loadCustomers(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load CUSTOMER HISTORY {}", w_id);
                            }
                            // CUSTOMER HISTORY
                            loadCustomerHistory(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load ORDERS {}", w_id);
                            }
                            // ORDERS
                            loadOpenOrders(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load NEW ORDERS {}", w_id);
                            }
                            // NEW ORDERS
                            loadNewOrders(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Starting to load ORDER LINES {}", w_id);
                            }
                            // ORDER LINES
                            loadOrderLines(ydbConnHelper, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                        }
                    }
                }

                @Override
                public void beforeLoad() {

                    // Make sure that we load the ITEM table first

                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            threads.add(t);
        }
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        return conn.prepareStatement(sql);
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


    protected void loadWarehouse(Connection conn, int w_id) {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Double,p3:Double,p4:Utf8," +
            "p5:Utf8,p6:Utf8,p7:Utf8,p8:Utf8,p9:Utf8>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as W_ID, $row.p2 as W_YTD, $row.p3 as W_TAX, " +
            "$row.p4 as W_NAME, $row.p5 as W_STREET_1, $row.p6 as W_STREET_2, " +
            "$row.p7 as W_CITY, $row.p8 as W_STATE, $row.p9 as W_ZIP));\n" +
            "upsert into " + TPCCConstants.TABLENAME_WAREHOUSE + " select * from as_table(ListMap($values, $mapper));";

        try {
            executeWithRetry(() -> {
                try (PreparedStatement whsePrepStmt = conn.prepareStatement(sql)) {
                    Warehouse warehouse = new Warehouse();

                    warehouse.w_id = w_id;
                    warehouse.w_ytd = 300000;

                    // random within [0.0000 .. 0.2000]
                    warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
                    warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
                    warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
                    warehouse.w_zip = "123456789";

                    int idx = 1;
                    whsePrepStmt.setInt(idx++, warehouse.w_id);
                    whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
                    whsePrepStmt.setDouble(idx++, warehouse.w_tax);
                    whsePrepStmt.setString(idx++, warehouse.w_name);
                    whsePrepStmt.setString(idx++, warehouse.w_street_1);
                    whsePrepStmt.setString(idx++, warehouse.w_street_2);
                    whsePrepStmt.setString(idx++, warehouse.w_city);
                    whsePrepStmt.setString(idx++, warehouse.w_state);
                    whsePrepStmt.setString(idx, warehouse.w_zip);
                    executeWithRetry(whsePrepStmt);

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadStock(YDBConnectionHelper ydbConnHelper, int w_id, int numItems) {
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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_STOCK, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadDistricts(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse) {
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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_DISTRICT, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadCustomers(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_CUSTOMER, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    protected void loadCustomerHistory(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
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
            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    long millis = System.currentTimeMillis();
                    Timestamp sysdate = new Timestamp(millis);
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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_HISTORY, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadOpenOrders(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
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

    protected void loadNewOrders(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
        final Map<String, Type> ydbTypes = new HashMap<>();
        ydbTypes.put("NO_W_ID", PrimitiveType.Int32);
        ydbTypes.put("NO_D_ID", PrimitiveType.Int32);
        ydbTypes.put("NO_O_ID", PrimitiveType.Int32);

        StructType structType = StructType.of(ydbTypes);
        List<Value<?>> batch = new ArrayList<>();

        try {

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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_NEWORDER, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void loadOrderLines(YDBConnectionHelper ydbConnHelper, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
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

            if (batch.size() > 0) {
                executeBulkUpsert(ydbConnHelper, TPCCConstants.TABLENAME_ORDERLINE, structType, batch);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }

    }

    protected void executeWithRetry(Runnable execution) throws SQLException {
        final int maxRetries = 10;
        final int waitTimeCeilingMs = 5000;

        int retryCount = 0;
        int waitTimeMs = 100;
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
                    Thread.sleep(waitTimeMs);
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

    protected void loadStock(Connection conn, int w_id, int numItems) {

        int k = 0;
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Double,p5:Int32," +
            "p6:Int32,p7:Utf8,p8:Utf8,p9:Utf8,p10:Utf8,p11:Utf8,p12:Utf8,p13:Utf8," +
            "p14:Utf8,p15:Utf8,p16:Utf8,p17:Utf8>>;\n" +
            "$mapper = ($row) -> (AsStruct(" +
            "$row.p1 as S_W_ID, $row.p2 as S_I_ID, $row.p3 as S_QUANTITY, $row.p4 as S_YTD, $row.p5 as S_ORDER_CNT, " +
            "$row.p6 as S_REMOTE_CNT, $row.p7 as S_DATA, $row.p8 as S_DIST_01, $row.p9 as S_DIST_02, " +
            "$row.p10 as S_DIST_03, $row.p11 as S_DIST_04, $row.p12 as S_DIST_05, $row.p13 as S_DIST_06, " +
            "$row.p14 as S_DIST_07, $row.p15 as S_DIST_08, $row.p16 as S_DIST_09, $row.p17 as S_DIST_10));\n" +
            "upsert into " + TPCCConstants.TABLENAME_STOCK + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement stockPreparedStatement = conn.prepareStatement(sql)) {

            for (int i = 1; i <= numItems; i++) {
                Stock stock = new Stock();
                stock.s_i_id = i;
                stock.s_w_id = w_id;
                stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
                stock.s_ytd = 0;
                stock.s_order_cnt = 0;
                stock.s_remote_cnt = 0;

                // s_data
                int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 ..
                    // 50]
                    stock.s_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere
                    // in middle
                    int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                int idx = 1;
                stockPreparedStatement.setInt(idx++, stock.s_w_id);
                stockPreparedStatement.setInt(idx++, stock.s_i_id);
                stockPreparedStatement.setInt(idx++, stock.s_quantity);
                stockPreparedStatement.setDouble(idx++, stock.s_ytd);
                stockPreparedStatement.setInt(idx++, stock.s_order_cnt);
                stockPreparedStatement.setInt(idx++, stock.s_remote_cnt);
                stockPreparedStatement.setString(idx++, stock.s_data);
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
                stockPreparedStatement.setString(idx, TPCCUtil.randomStr(24));
                stockPreparedStatement.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                    executeBatchWithRetry(stockPreparedStatement);
                }
            }

            executeBatchWithRetry(stockPreparedStatement);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadDistricts(Connection conn, int w_id, int districtsPerWarehouse) {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Double,p4:Double,p5:Int32," +
            "p6:Utf8,p7:Utf8,p8:Utf8,p9:Utf8,p10:Utf8,p11:Utf8>>;\n" +
            "$mapper = ($row) -> (AsStruct(" +
            "$row.p1 as D_W_ID, $row.p2 as D_ID, $row.p3 as D_YTD, $row.p4 as D_TAX, $row.p5 as D_NEXT_O_ID, " +
            "$row.p6 as D_NAME, $row.p7 as D_STREET_1, $row.p8 as D_STREET_2, $row.p9 as D_CITY, " +
            "$row.p10 as D_STATE, $row.p11 as D_ZIP));\n" +
            "upsert into " + TPCCConstants.TABLENAME_DISTRICT + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement distPrepStmt = conn.prepareStatement(sql)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                District district = new District();
                district.d_id = d;
                district.d_w_id = w_id;
                district.d_ytd = 30000;

                // random within [0.0000 .. 0.2000]
                district.d_tax = (double) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

                district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
                district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
                district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                district.d_state = TPCCUtil.randomStr(3).toUpperCase();
                district.d_zip = "123456789";

                int idx = 1;
                distPrepStmt.setInt(idx++, district.d_w_id);
                distPrepStmt.setInt(idx++, district.d_id);
                distPrepStmt.setDouble(idx++, district.d_ytd);
                distPrepStmt.setDouble(idx++, district.d_tax);
                distPrepStmt.setInt(idx++, district.d_next_o_id);
                distPrepStmt.setString(idx++, district.d_name);
                distPrepStmt.setString(idx++, district.d_street_1);
                distPrepStmt.setString(idx++, district.d_street_2);
                distPrepStmt.setString(idx++, district.d_city);
                distPrepStmt.setString(idx++, district.d_state);
                distPrepStmt.setString(idx, district.d_zip);
                executeUpdateWithRetry(distPrepStmt);
            }

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;

        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Double,p5:Utf8," +
            "p6:Utf8,p7:Utf8,p8:Double,p9:Double,p10:Double,p11:Int32,p12:Int32,p13:Utf8," +
            "p14:Utf8,p15:Utf8,p16:Utf8,p17:Utf8,p18:Utf8,p19:Timestamp,p20:Utf8,p21:Utf8>>;\n" +
            "$mapper = ($row) -> (AsStruct(" +
            "$row.p1 as C_W_ID, $row.p2 as C_D_ID, $row.p3 as C_ID, $row.p4 as C_DISCOUNT, $row.p5 as C_CREDIT, " +
            "$row.p6 as C_LAST, $row.p7 as C_FIRST, $row.p8 as C_CREDIT_LIM, $row.p9 as C_BALANCE, " +
            "$row.p10 as C_YTD_PAYMENT, $row.p11 as C_PAYMENT_CNT, $row.p12 as C_DELIVERY_CNT, " +
            "$row.p13 as C_STREET_1, $row.p14 as C_STREET_2, $row.p15 as C_CITY, $row.p16 as C_STATE, " +
            "$row.p17 as C_ZIP, $row.p18 as C_PHONE, $row.p19 as C_SINCE, " +
            "$row.p20 as C_MIDDLE, $row.p21 as C_DATA));\n" +
            "upsert into " + TPCCConstants.TABLENAME_CUSTOMER + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement custPrepStmt = conn.prepareStatement(sql)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                    Customer customer = new Customer();
                    customer.c_id = c;
                    customer.c_d_id = d;
                    customer.c_w_id = w_id;

                    // discount is random between [0.0000 ... 0.5000]
                    customer.c_discount = (double) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

                    if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
                        customer.c_credit = "BC"; // 10% Bad Credit
                    } else {
                        customer.c_credit = "GC"; // 90% Good Credit
                    }
                    if (c <= 1000) {
                        customer.c_last = TPCCUtil.getLastName(c - 1);
                    } else {
                        customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
                    }
                    customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
                    customer.c_credit_lim = 50000;

                    customer.c_balance = -10;
                    customer.c_ytd_payment = 10;
                    customer.c_payment_cnt = 1;
                    customer.c_delivery_cnt = 0;

                    customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
                    customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
                    // TPC-C 4.3.2.7: 4 random digits + "11111"
                    customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
                    customer.c_phone = TPCCUtil.randomNStr(16);
                    customer.c_since = sysdate;
                    customer.c_middle = "OE";
                    customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

                    int idx = 1;
                    custPrepStmt.setInt(idx++, customer.c_w_id);
                    custPrepStmt.setInt(idx++, customer.c_d_id);
                    custPrepStmt.setInt(idx++, customer.c_id);
                    custPrepStmt.setDouble(idx++, customer.c_discount);
                    custPrepStmt.setString(idx++, customer.c_credit);
                    custPrepStmt.setString(idx++, customer.c_last);
                    custPrepStmt.setString(idx++, customer.c_first);
                    custPrepStmt.setDouble(idx++, customer.c_credit_lim);
                    custPrepStmt.setDouble(idx++, customer.c_balance);
                    custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
                    custPrepStmt.setInt(idx++, customer.c_payment_cnt);
                    custPrepStmt.setInt(idx++, customer.c_delivery_cnt);
                    custPrepStmt.setString(idx++, customer.c_street_1);
                    custPrepStmt.setString(idx++, customer.c_street_2);
                    custPrepStmt.setString(idx++, customer.c_city);
                    custPrepStmt.setString(idx++, customer.c_state);
                    custPrepStmt.setString(idx++, customer.c_zip);
                    custPrepStmt.setString(idx++, customer.c_phone);
                    custPrepStmt.setTimestamp(idx++, customer.c_since);
                    custPrepStmt.setString(idx++, customer.c_middle);
                    custPrepStmt.setString(idx, customer.c_data);
                    custPrepStmt.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        executeBatchWithRetry(custPrepStmt);
                    }
                }
            }

            executeBatchWithRetry(custPrepStmt);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadCustomerHistory(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32," +
            "p5:Int32,p6:Timestamp,p7:Double,p8:Utf8,p9:Int64>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as H_C_ID, $row.p2 as H_C_D_ID, $row.p3 as H_C_W_ID, " +
            "$row.p4 as H_D_ID, $row.p5 as H_W_ID, $row.p6 as H_DATE, $row.p7 as H_AMOUNT, "+
            "$row.p8 as H_DATA, $row.p9 as H_C_NANO_TS));\n" +
            "upsert into " + TPCCConstants.TABLENAME_HISTORY + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement histPrepStmt = conn.prepareStatement(sql)) {

            long prevTs = 0;
            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                    long ts = System.nanoTime();
                    if (ts <= prevTs) {
                        ts = prevTs + 1;
                    }

                    History history = new History();
                    history.h_c_id = c;
                    history.h_c_d_id = d;
                    history.h_c_w_id = w_id;
                    history.h_d_id = d;
                    history.h_w_id = w_id;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()));


                    int idx = 1;
                    histPrepStmt.setInt(idx++, history.h_c_id);
                    histPrepStmt.setInt(idx++, history.h_c_d_id);
                    histPrepStmt.setInt(idx++, history.h_c_w_id);
                    histPrepStmt.setInt(idx++, history.h_d_id);
                    histPrepStmt.setInt(idx++, history.h_w_id);
                    histPrepStmt.setTimestamp(idx++, history.h_date);
                    histPrepStmt.setDouble(idx++, history.h_amount);
                    histPrepStmt.setString(idx++, history.h_data);
                    histPrepStmt.setLong(idx, ts);
                    histPrepStmt.addBatch();

                    prevTs = ts;

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        executeBatchWithRetry(histPrepStmt);
                    }
                }
            }

            executeBatchWithRetry(histPrepStmt);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadOpenOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32,p5:Int32," +
            "p6:Int32,p7:Int32,p8:Timestamp>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as O_W_ID, $row.p2 as O_D_ID, $row.p3 as O_ID, " +
            "$row.p4 as O_C_ID, $row.p5 as O_CARRIER_ID, " +
            "$row.p6 as O_OL_CNT, $row.p7 as O_ALL_LOCAL, $row.p8 as O_ENTRY_D));\n" +
            "upsert into " + TPCCConstants.TABLENAME_OPENORDER + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement openOrderStatement = conn.prepareStatement(sql)) {

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

                    Oorder oorder = new Oorder();
                    oorder.o_id = c;
                    oorder.o_w_id = w_id;
                    oorder.o_d_id = d;
                    oorder.o_c_id = c_ids[c - 1];
                    // o_carrier_id is set *only* for orders with ids < 2101
                    // [4.3.3.1]
                    if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                        oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
                    } else {
                        oorder.o_carrier_id = 0;
                    }
                    oorder.o_ol_cnt = getRandomCount(w_id, c, d);
                    oorder.o_all_local = 1;
                    oorder.o_entry_d = new Timestamp(System.currentTimeMillis());


                    int idx = 1;
                    openOrderStatement.setInt(idx++, oorder.o_w_id);
                    openOrderStatement.setInt(idx++, oorder.o_d_id);
                    openOrderStatement.setInt(idx++, oorder.o_id);
                    openOrderStatement.setInt(idx++, oorder.o_c_id);
                    if (oorder.o_carrier_id != null) {
                        openOrderStatement.setInt(idx++, oorder.o_carrier_id);
                    } else {
                        openOrderStatement.setNull(idx++, Types.INTEGER);
                    }
                    openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
                    openOrderStatement.setInt(idx++, oorder.o_all_local);
                    openOrderStatement.setTimestamp(idx, oorder.o_entry_d);
                    openOrderStatement.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        executeBatchWithRetry(openOrderStatement);
                    }

                }

            }

            executeBatchWithRetry(openOrderStatement);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadNewOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as NO_W_ID, $row.p2 as NO_D_ID, $row.p3 as NO_O_ID));\n" +
            "upsert into " + TPCCConstants.TABLENAME_NEWORDER + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement newOrderStatement = conn.prepareStatement(sql)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    // 900 rows in the NEW-ORDER table corresponding to the last
                    // 900 rows in the ORDER table for that district (i.e.,
                    // with NO_O_ID between 2,101 and 3,000)
                    if (c >= FIRST_UNPROCESSED_O_ID) {
                        NewOrder new_order = new NewOrder();
                        new_order.no_w_id = w_id;
                        new_order.no_d_id = d;
                        new_order.no_o_id = c;

                        int idx = 1;
                        newOrderStatement.setInt(idx++, new_order.no_w_id);
                        newOrderStatement.setInt(idx++, new_order.no_d_id);
                        newOrderStatement.setInt(idx, new_order.no_o_id);
                        newOrderStatement.addBatch();

                        k++;
                    }

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        executeBatchWithRetry(newOrderStatement);
                    }

                }

            }

            executeBatchWithRetry(newOrderStatement);


        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void loadOrderLines(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

        int k = 0;
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32,p5:Int32," +
            "p6:Timestamp,p7:Double,p8:Int32,p9:Double,p10:Utf8>>;\n" +
            "$mapper = ($row) -> (AsStruct(" +
            "$row.p1 as OL_W_ID, $row.p2 as OL_D_ID, $row.p3 as OL_O_ID, $row.p4 as OL_NUMBER, $row.p5 as OL_I_ID, " +
            "$row.p6 as OL_DELIVERY_D, $row.p7 as OL_AMOUNT, $row.p8 as OL_SUPPLY_W_ID, $row.p9 as OL_QUANTITY, " +
            "$row.p10 as OL_DIST_INFO));\n" +
            "upsert into " + TPCCConstants.TABLENAME_ORDERLINE + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement orderLineStatement = conn.prepareStatement(sql)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    int count = getRandomCount(w_id, c, d);

                    for (int l = 1; l <= count; l++) {
                        OrderLine order_line = new OrderLine();
                        order_line.ol_w_id = w_id;
                        order_line.ol_d_id = d;
                        order_line.ol_o_id = c;
                        order_line.ol_number = l; // ol_number
                        order_line.ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
                        if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                            order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
                            order_line.ol_amount = 0;
                        } else {
                            order_line.ol_delivery_d = new Timestamp(0);
                            // random within [0.01 .. 9,999.99]
                            order_line.ol_amount = (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
                        }
                        order_line.ol_supply_w_id = order_line.ol_w_id;
                        order_line.ol_quantity = 5;
                        order_line.ol_dist_info = TPCCUtil.randomStr(24);

                        int idx = 1;
                        orderLineStatement.setInt(idx++, order_line.ol_w_id);
                        orderLineStatement.setInt(idx++, order_line.ol_d_id);
                        orderLineStatement.setInt(idx++, order_line.ol_o_id);
                        orderLineStatement.setInt(idx++, order_line.ol_number);
                        orderLineStatement.setInt(idx++, order_line.ol_i_id);
                        orderLineStatement.setTimestamp(idx++, order_line.ol_delivery_d);
                        orderLineStatement.setDouble(idx++, order_line.ol_amount);
                        orderLineStatement.setInt(idx++, order_line.ol_supply_w_id);
                        orderLineStatement.setDouble(idx++, order_line.ol_quantity);
                        orderLineStatement.setString(idx, order_line.ol_dist_info);
                        orderLineStatement.addBatch();

                        k++;

                        if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                            executeBatchWithRetry(orderLineStatement);
                        }

                    }

                }

            }

            executeBatchWithRetry(orderLineStatement);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            System.exit(1);
        }

    }

    protected void executeBulkUpsert(YDBConnectionHelper ydbConnHelper, String tableName, StructType structType, List<Value<?>> batch) {
        if (batch.size() == 0) {
            return;
        }

        ListValue bulkData = ListType.of(structType).newValue(batch);
        String path = String.format("%s/%s", ydbConnHelper.getDatabase(), tableName);
        batch.clear();

        try {
            CompletableFuture<tech.ydb.core.Status> future =
                ydbConnHelper.retryCtx.supplyStatus(session -> session.executeBulkUpsert(path, bulkData));

            future.join().expectSuccess(String.format("bulk upsert problem, table: %s", tableName));
        } catch (Exception e) {
            LOG.error(String.format("Error executing bulk upsert, table: %s, exception: %s", tableName, e.getMessage()));
            throw e;
        }
    }

    protected void executeBatchWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.executeBatch();
                statement.clearBatch();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    protected void executeWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.execute();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    protected void executeUpdateWithRetry(PreparedStatement statement) throws SQLException {
        executeWithRetry(() -> {
            try {
                statement.executeUpdate();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
