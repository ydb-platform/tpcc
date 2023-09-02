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

import com.oltpbenchmark.api.TransactionTypes;

import java.util.Arrays;

public class ResultStats {
    TransactionTypes transactionTypes;

    int transactionTypeCount;
    TransactionStats[] transactionStats;

    public ResultStats() {
    }

    public ResultStats(TransactionTypes transactionTypes) {
        this.transactionTypes = transactionTypes;
        this.transactionTypeCount = transactionTypes.size();
        assert (this.transactionTypeCount > 0);

        this.transactionStats = new TransactionStats[this.transactionTypeCount];
        for (int i = 0; i < transactionTypeCount; i++) {
            transactionStats[i] = new TransactionStats();
        }
    }

    public void addLatency(int transType, long startNanosecond, long endNanosecond, boolean isSuccess) {
        assert (transType < transactionTypeCount);
        transactionStats[transType].addLatency(startNanosecond, endNanosecond, isSuccess);
    }

    public void add(ResultStats another) {
        if (this.transactionTypeCount == 0) {
            this.transactionTypes = another.transactionTypes;
            this.transactionTypeCount = another.transactionTypeCount;
            this.transactionStats = another.transactionStats;
            return;
        }

        assert (this.transactionTypeCount == another.transactionTypeCount);

        for (int i = 0; i < transactionTypeCount; i++) {
            transactionStats[i].add(another.transactionStats[i]);
        }
    }

    public long count() {
        long total = 0;
        for (int i = 0; i < transactionTypeCount; i++) {
            total += transactionStats[i].count();
        }

        return total;
    }

    public long getSuccessCount(int transType) {
        assert (transType < transactionTypeCount);
        return transactionStats[transType].getSuccessCount();
    }

    public String toJson() {
        StringBuilder json = new StringBuilder();
        json.append("{");

        for (int i = 0; i < transactionTypeCount; i++) {
            if (i != 0) {
                json.append(",\"");
            } else {
                json.append("\"");
            }
            json.append(transactionTypes.getType(i).getName());
            json.append("\": ");
            json.append(transactionStats[i].toJson());
        }

        json.append("}");
        return json.toString();
    }

    @Override
    public String toString() {
        StringBuilder reprStr = new StringBuilder();
        for (int i = 0; i < transactionTypeCount; i++) {
            reprStr.append("\n");
            reprStr.append(transactionTypes.getType(i).getName());
            reprStr.append(":\n");
            reprStr.append(transactionStats[i].toString());
        }

        return reprStr.toString();
    }

    public class Histogram {
        private int[] bucketlist;
        private int[] buckets;

        public Histogram() {
            // note, that because 5000 is here, we can calculate precisely number of transactions below
            // this value.
            this(new int[]{1, 5, 10, 50, 100, 500, 1000, 2000, 3000, 4000, 4500, 5000, 6000, 10000});
        }

        public Histogram(int[] bucketlist) {
            Arrays.sort(bucketlist);
            this.bucketlist = bucketlist;
            this.buckets = new int[bucketlist.length + 1];
        }

        public void add(int value) {
            int i = Arrays.binarySearch(bucketlist, value);
            if (i < 0) {
                i = -i - 1;
            } else {
                i += 1;
            }
            buckets[i]++;
        }

        public int count(int value) {
            int i = Arrays.binarySearch(bucketlist, value);
            if (i < 0) {
                i = -i - 1;
            } else {
                i += 1;
            }
            return buckets[i];
        }

        public int totalCount() {
            return Arrays.stream(buckets).sum();
        }

        public String percentile(double percentile) {
            int total = totalCount();
            int cumulative = 0;
            for (int i = 0; i < buckets.length; i++) {
                cumulative += buckets[i];
                if ((double) cumulative / total >= percentile / 100.0) {
                    return i != 0 ? Integer.toString(bucketlist[i - 1]) : "<" + bucketlist[0];
                }
            }
            return ">= " + bucketlist[bucketlist.length - 1];
        }

        public void add(Histogram another) {
            if (another.bucketlist.length != this.bucketlist.length) {
                throw new IllegalArgumentException("Bucketlists must be of the same length");
            }

            for (int i = 0; i < this.bucketlist.length; i++) {
                if (this.bucketlist[i] != another.bucketlist[i]) {
                    throw new IllegalArgumentException("Bucketlists must have the same values");
                }
            }

            for (int i = 0; i < this.buckets.length; i++) {
                this.buckets[i] += another.buckets[i];
            }
        }

        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{");

            // Add bucketlist array to JSON
            json.append("\"bucketlist\": [");
            for (int i = 0; i < bucketlist.length; i++) {
                json.append(bucketlist[i]);
                if (i < bucketlist.length - 1) {
                    json.append(", ");
                }
            }
            json.append("], ");

            // Add buckets array to JSON
            json.append("\"buckets\": [");
            for (int i = 0; i < buckets.length; i++) {
                json.append(buckets[i]);
                if (i < buckets.length - 1) {
                    json.append(", ");
                }
            }
            json.append("]");

            json.append("}");
            return json.toString();
        }

        @Override
        public String toString() {
            StringBuilder reprStr = new StringBuilder();
            for (int i = 0; i < buckets.length; i++) {
                if (i == 0) {
                    reprStr.append("<").append(bucketlist[0]).append(": ").append(buckets[i]).append(", ");
                } else if (i == buckets.length - 1) {
                    reprStr.append(">=").append(bucketlist[i - 1]).append(": ").append(buckets[i]);
                } else {
                    reprStr.append(bucketlist[i - 1]).append("-").append(bucketlist[i]).append(": ").append(buckets[i]).append(", ");
                }
            }
            return reprStr.toString();
        }
    }

    public class TransactionStats {
        long successCount;
        long failedCount;

        Histogram latencyHistogramMs;

        public TransactionStats() {
            this.latencyHistogramMs = new Histogram();
        }

        public void addLatency(long startNanosecond, long endNanosecond, boolean isSuccess) {
            latencyHistogramMs.add((int) ((endNanosecond - startNanosecond) / 1000));

            if (isSuccess) {
                successCount++;
            } else {
                failedCount++;
            }
        }

        public void add(TransactionStats another) {
            this.successCount += another.successCount;
            this.failedCount += another.failedCount;
            this.latencyHistogramMs.add(another.latencyHistogramMs);
        }

        public long count() {
            return successCount + failedCount;
        }

        public long getSuccessCount() {
            return successCount;
        }

        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{");

            // Add SuccessCount to JSON
            json.append("\"SuccessCount\": ").append(successCount).append(", ");

            // Add FailureCount to JSON
            json.append("\"FailureCount\": ").append(failedCount).append(", ");

            // Add LatencyHistogramMs to JSON
            json.append("\"LatencyHistogramMs\": ").append(latencyHistogramMs.toJson());

            json.append("}");
            return json.toString();
        }

        @Override
        public String toString() {
            StringBuilder reprStr = new StringBuilder();
            reprStr.append("Success: ");
            reprStr.append(successCount);
            reprStr.append("\nFailed: ");
            reprStr.append(failedCount);
            reprStr.append("\nLatency histogram, ms:\n");
            reprStr.append(latencyHistogramMs.toString());

            return reprStr.toString();
        }
    }

}
