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

    public static class Histogram {
        private int[] bucketlist;
        private int[] buckets;

        public Histogram() {
            // note, that because 5000 is here, we can calculate precisely number of transactions below
            // this value.
            this(new int[]{
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                16, 32, 64, 128, 256, 512,
                1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000,
                7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000,
                20000
            });
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

            json.append("\"bucketlist\": [");
            for (int i = 0; i < bucketlist.length; i++) {
                json.append(bucketlist[i]);
                if (i < bucketlist.length - 1) {
                    json.append(", ");
                }
            }
            json.append("], ");

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

    public static class TransactionStats {
        long successCount;
        long failedCount;

        Histogram latencySuccessHistogramMs;
        Histogram latencyFailedHistogramMs;

        public TransactionStats() {
            this.latencySuccessHistogramMs = new Histogram();
            this.latencyFailedHistogramMs = new Histogram();
        }

        public void addLatency(long startNanosecond, long endNanosecond, boolean isSuccess) {
            int deltaMs = (int) ((endNanosecond - startNanosecond) / 1000000);
            if (isSuccess) {
                latencySuccessHistogramMs.add(deltaMs);
                successCount++;
            } else {
                latencyFailedHistogramMs.add(deltaMs);
                failedCount++;
            }
        }

        public void add(TransactionStats another) {
            this.successCount += another.successCount;
            this.failedCount += another.failedCount;
            this.latencySuccessHistogramMs.add(another.latencySuccessHistogramMs);
            this.latencyFailedHistogramMs.add(another.latencyFailedHistogramMs);
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

            json.append("\"SuccessCount\": ").append(successCount).append(", ");
            json.append("\"FailureCount\": ").append(failedCount).append(", ");

            json.append("\"LatencySuccessHistogramMs\": ").append(latencySuccessHistogramMs.toJson());
            json.append(", \"LatencyFailedHistogramMs\": ").append(latencyFailedHistogramMs.toJson());

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

            reprStr.append("\nLatency success histogram, ms:\n");
            reprStr.append(latencySuccessHistogramMs.toString());

            reprStr.append("\nLatency failed histogram, ms:\n");
            reprStr.append(latencyFailedHistogramMs.toString());

            return reprStr.toString();
        }
    }

}
