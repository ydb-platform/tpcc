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

import com.oltpbenchmark.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ThreadLocalRandom;

public class Phase {
    public enum Arrival {
        REGULAR, POISSON,
    }

    private final String benchmarkName;
    private final int id;
    private final int time;
    private final int warmupTime;
    private final Arrival arrival;

    private final boolean timed;
    private final List<Double> weights;
    private final int totalWeight;
    private final int weightCount;
    private final int activeTerminals;

    Phase(String benchmarkName, int id, int t, int wt, List<Double> weights, boolean timed, int activeTerminals, Arrival a) {
        this.benchmarkName = benchmarkName;
        this.id = id;
        this.time = t;
        this.warmupTime = wt;
        this.weights = weights;
        this.weightCount = this.weights.size();
        this.timed = timed;
        this.activeTerminals = activeTerminals;
        this.arrival = a;

        double total = 0;
        for (Double d : weights) {
            total += d;
        }
        this.totalWeight = (int)total;
    }

    public boolean isTimed() {
        return timed;
    }

    public int getActiveTerminals() {
        return activeTerminals;
    }

    public int getWeightCount() {
        return (this.weightCount);
    }

    public int getId() {
        return id;
    }

    public int getTime() {
        return time;
    }

    public int getWarmupTime() {
        return warmupTime;
    }

    public Arrival getArrival() {
        return arrival;
    }

    public List<Double> getWeights() {
        return (this.weights);
    }

    /**
     * This simply computes the next transaction by randomly selecting one based
     * on the weights of this phase.
     *
     * @return
     */
    public int chooseTransaction() {
        int randomPercentage = ThreadLocalRandom.current().nextInt(totalWeight) + 1;
        double weight = 0.0;
        for (int i = 0; i < this.weightCount; i++) {
            weight += weights.get(i);
            if (randomPercentage <= weight) {
                return i + 1;
            }
        }

        return -1;
    }

    /**
     * Returns a string for logging purposes when entering the phase
     */
    public String currentPhaseString() {
        List<String> inner = new ArrayList<>();
        inner.add("[Workload=" + benchmarkName.toUpperCase() + "]");
        inner.add("[Time=" + time + "]");
        inner.add("[WarmupTime=" + warmupTime + "]");
        inner.add("[Arrival=" + arrival + "]");
        inner.add("[Ratios=" + getWeights() + "]");
        inner.add("[ActiveWorkers=" + getActiveTerminals() + "]");

        return StringUtil.bold("PHASE START") + " :: " + StringUtil.join(" ", inner);
    }

}