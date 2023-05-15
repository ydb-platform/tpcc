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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

public class Delivery extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Delivery.class);

    public SQLStmt delivGetOrderIdSQL = new SQLStmt(
        """
            SELECT NO_O_ID FROM %s
             WHERE NO_D_ID = ?
               AND NO_W_ID = ?
             ORDER BY NO_O_ID ASC
             LIMIT 1
        """.formatted(TPCCConstants.TABLENAME_NEWORDER));

    public SQLStmt delivGetCustIdSQL = new SQLStmt(
        """
            SELECT O_C_ID FROM %s
            WHERE O_ID = ?
            AND O_D_ID = ?
            AND O_W_ID = ?
        """.formatted(TPCCConstants.TABLENAME_OPENORDER));

    public SQLStmt delivOrderLineData = new SQLStmt(
    """
        SELECT OL_NUMBER, OL_AMOUNT
          FROM %s
         WHERE OL_O_ID = ?
           AND OL_D_ID = ?
           AND OL_W_ID = ?
    """.formatted(TPCCConstants.TABLENAME_ORDERLINE));

    public SQLStmt delivGetCustomerData = new SQLStmt(
    """
        SELECT C_BALANCE, C_DELIVERY_CNT
          FROM %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
    """.formatted(TPCCConstants.TABLENAME_CUSTOMER));

    private class OrderLineData {
        double TotalAmount;
        ArrayList<Integer> LineNumbers;

        OrderLineData() {
            this.TotalAmount = 0;
            this.LineNumbers = new ArrayList<>();
        }
    }

    private class CustomerData {
        int Id;
        double balance;
        int deliveryCnt;
    }

    private class Data {
        int orderId;
        CustomerData customerData;
        OrderLineData orderLineData;
    }

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

        int o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);

        int d_id;


        // note that we at first read everything and then update.
        // This helps to avoid requirement on transaction to be able to
        // read own modifications

        Data[] orders = new Data[TPCCConfig.configDistPerWhse];
        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {
            Integer no_o_id = getOrderId(conn, w_id, d_id);

            if (no_o_id == null) {
                orders[d_id - 1] = null;
                continue;
            }

            orders[d_id - 1] = new Data();
            orders[d_id - 1].orderId = no_o_id;
            orders[d_id - 1].customerData = getCustomerData(conn, w_id, d_id, no_o_id);
            orders[d_id - 1].orderLineData = getOrderLineData(conn, w_id, d_id, no_o_id);
        }

        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {
            if (orders[d_id - 1] == null) {
                continue;
            }

            deleteOrder(conn, w_id, d_id, orders[d_id - 1].orderId);
            updateCarrierId(conn, w_id, o_carrier_id, d_id, orders[d_id - 1].orderId);
            updateDeliveryDate(conn, w_id, d_id, orders[d_id - 1]);
            updateBalanceAndDelivery(conn, w_id, d_id, orders[d_id - 1]);
        }

        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
            terminalMessage.append(" Date: ");
            terminalMessage.append(TPCCUtil.getCurrentTime());
            terminalMessage.append("\n\n Warehouse: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n Carrier:   ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n Delivered Orders\n");
            for (int i = 1; i <= TPCCConfig.configDistPerWhse; i++) {
                if (orders[i - 1] != null && orders[i - 1].orderId  >= 0) {
                    terminalMessage.append("  District ");
                    terminalMessage.append(i < 10 ? " " : "");
                    terminalMessage.append(i);
                    terminalMessage.append(": Order number ");
                    terminalMessage.append(orders[i - 1].orderId);
                    terminalMessage.append(" was delivered.\n");
                }
            }
            terminalMessage.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(terminalMessage.toString());
        }

    }

    private Integer getOrderId(Connection conn, int w_id, int d_id) throws SQLException {

        try (PreparedStatement delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL)) {
            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);

            try (ResultSet rs = delivGetOrderId.executeQuery()) {

                if (!rs.next()) {
                    // This district has no new orders.  This can happen but should be rare

                    LOG.warn(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));

                    return null;
                }

                return rs.getInt("NO_O_ID");

            }
        }
    }

    private void deleteOrder(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as NO_O_ID, $row.p2 as NO_D_ID, $row.p3 as NO_W_ID));\n" +
            "delete from " + TPCCConstants.TABLENAME_NEWORDER + " on select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement delivDeleteNewOrder = conn.prepareStatement(sql)) {
            delivDeleteNewOrder.setInt(1, no_o_id);
            delivDeleteNewOrder.setInt(2, d_id);
            delivDeleteNewOrder.setInt(3, w_id);

            int result = delivDeleteNewOrder.executeUpdate();

            if (result != 1) {
                // This code used to run in a loop in an attempt to make this work
                // with MySQL's default weird consistency level. We just always run
                // this as SERIALIZABLE instead. I don't *think* that fixing this one
                // error makes this work with MySQL's default consistency.
                // Careful auditing would be required.
                String msg = String.format("NewOrder delete failed. Not running with SERIALIZABLE isolation? [w_id=%d, d_id=%d, no_o_id=%d]", w_id, d_id, no_o_id);
                throw new UserAbortException(msg);
            }
        }
    }

    private CustomerData getCustomerData(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        CustomerData data = new CustomerData();

        try (PreparedStatement delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL)) {
            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);

            try (ResultSet rs = delivGetCustId.executeQuery()) {

                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }

                data.Id = rs.getInt("O_C_ID");
            }
        }


        // read customer data
        try (PreparedStatement delivGetCustData = this.getPreparedStatement(conn, delivGetCustomerData)) {
            delivGetCustData.setInt(1, w_id);
            delivGetCustData.setInt(2, d_id);
            delivGetCustData.setInt(3, data.Id);

            try (ResultSet rs = delivGetCustData.executeQuery()) {

                if (!rs.next()) {
                    String msg = String.format(
                        "Failed to retrieve CUSTOMER record [W_ID=%d, D_ID=%d, C_ID=%d]",
                            w_id, d_id, data.Id);
                    throw new RuntimeException(msg);
                }

                data.balance = rs.getDouble("C_BALANCE");
                data.deliveryCnt = rs.getInt("C_DELIVERY_CNT");
            }
        }

        return data;
    }

    private void updateCarrierId(Connection conn, int w_id, int o_carrier_id, int d_id, int no_o_id) throws SQLException {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as O_CARRIER_ID, $row.p2 as O_ID, $row.p3 as O_D_ID, " +
            "$row.p4 as O_W_ID));\n" +
            "upsert into " + TPCCConstants.TABLENAME_OPENORDER + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement delivUpdateCarrierId = conn.prepareStatement(sql)) {
            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setInt(2, no_o_id);
            delivUpdateCarrierId.setInt(3, d_id);
            delivUpdateCarrierId.setInt(4, w_id);

            int result = delivUpdateCarrierId.executeUpdate();

            if (result != 1) {
                String msg = String.format("Failed to update ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                throw new RuntimeException(msg);
            }
        }
    }

    private void updateDeliveryDate(Connection conn, int w_id, int d_id, Data data) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String sql = "" +
            "declare $values as List<Struct<p1:Int,p2:Int32,p3:Int32,p4:Int32,p5:Timestamp>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as OL_W_ID, $row.p2 as OL_D_ID, $row.p3 as OL_O_ID, " +
            "$row.p4 as OL_NUMBER, $row.p5 as OL_DELIVERY_D));\n" +
            "upsert into " + TPCCConstants.TABLENAME_ORDERLINE + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement delivUpdateDeliveryDate = conn.prepareStatement(sql)) {
            for (int lineNum : data.orderLineData.LineNumbers) {
                int idx = 1;
                delivUpdateDeliveryDate.setInt(idx++, w_id);
                delivUpdateDeliveryDate.setInt(idx++, d_id);
                delivUpdateDeliveryDate.setInt(idx++, data.orderId);
                delivUpdateDeliveryDate.setInt(idx++, lineNum);
                delivUpdateDeliveryDate.setTimestamp(idx, timestamp);
                delivUpdateDeliveryDate.addBatch();
            }

            delivUpdateDeliveryDate.executeBatch();
            delivUpdateDeliveryDate.clearBatch();
        } catch (SQLException se) {
            String msg = String.format(
                "Failed to update ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]: %s", w_id, d_id, data.orderId, se);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }

    private OrderLineData getOrderLineData(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        try (PreparedStatement delivSumOrderAmount = this.getPreparedStatement(conn, delivOrderLineData)) {
            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);

            OrderLineData data = new OrderLineData();
            try (ResultSet rs = delivSumOrderAmount.executeQuery()) {
                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }

                do {
                    data.TotalAmount += rs.getDouble("OL_AMOUNT");
                    data.LineNumbers.add(rs.getInt("OL_NUMBER"));
                } while (rs.next());
            }

            return data;
        }
    }

    private void updateBalanceAndDelivery(Connection conn, int w_id, int d_id, Data data) throws SQLException {
        String sql = "" +
            "declare $values as List<Struct<p1:Int32,p2:Int32,p3:Int32,p4:Int32,p5:Double>>;\n" +
            "$mapper = ($row) -> (AsStruct($row.p1 as C_W_ID, $row.p2 as C_D_ID, $row.p3 as C_ID, " +
            "$row.p4 as C_DELIVERY_CNT, $row.p5 as C_BALANCE));\n" +
            "upsert into " + TPCCConstants.TABLENAME_CUSTOMER + " select * from as_table(ListMap($values, $mapper));";

        try (PreparedStatement delivUpdateCustBalDelivCnt = conn.prepareStatement(sql)) {
            int idx = 1;
            delivUpdateCustBalDelivCnt.setInt(idx++, w_id);
            delivUpdateCustBalDelivCnt.setInt(idx++, d_id);
            delivUpdateCustBalDelivCnt.setInt(idx++, data.customerData.Id);
            delivUpdateCustBalDelivCnt.setInt(idx++, data.customerData.deliveryCnt + 1);
            delivUpdateCustBalDelivCnt.setDouble(idx++, data.customerData.balance + data.orderLineData.TotalAmount);

            int result = delivUpdateCustBalDelivCnt.executeUpdate();

            if (result == 0) {
                String msg = String.format(
                    "Failed to update CUSTOMER record [W_ID=%d, D_ID=%d, C_ID=%d]",
                        w_id, d_id, data.customerData.Id);
                throw new RuntimeException(msg);
            }
        }
    }

}
