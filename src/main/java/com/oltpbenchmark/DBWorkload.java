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

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.*;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

import org.apache.commons.cli.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import tech.ydb.jdbc.YdbDriver;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.*;

public class DBWorkload {
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

    // FIXME: TPC-C only hack
    private static int newOrderTxnId = -1;
    private static int numWarehouses = 10;
    private static int time = 0;

    private static Boolean useRealThreads = false;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Enable redirect Java Util Logging to SLF4J
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("").setLevel(Level.FINEST);

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        XMLConfiguration pluginConfig = buildConfiguration("config/plugin.xml");

        Options options = buildOptions(pluginConfig);

        CommandLine argsLine = parser.parse(options, args);

        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("c")) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("b")) {
            LOG.error("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }


        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        int startFromId = 1;
        if (argsLine.hasOption("sf")) {
            startFromId = Integer.parseInt(argsLine.getOptionValue("sf"));
        }

        int totalWarehousesInCompany = 0;
        if (argsLine.hasOption("totalwh")) {
            totalWarehousesInCompany = Integer.parseInt(argsLine.getOptionValue("totalwh"));
        }

        if (argsLine.hasOption("rt")) {
            useRealThreads = true;
        }

        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------

        String targetBenchmarks = argsLine.getOptionValue("b");

        String[] targetList = targetBenchmarks.split(",");
        if (targetList.length == 0) {
            throw new ParseException("No benchmarks specified", 1);
        }

        if (targetList.length > 1) {
            throw new ParseException("Only one benchmark can be specified when exporting dialects", 1);
        }

        if (!targetList[0].equals("tpcc")) {
            throw new ParseException("Only TPC-C benchmark is supported", 1);
        }

        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<>();

        String configFile = argsLine.getOptionValue("c");

        XMLConfiguration xmlConfig = buildConfiguration(configFile);

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        final String plugin = "tpcc";
        String pluginTest = "[@bench='" + plugin + "']";

        // ----------------------------------------------------------------
        // BEGIN LOADING WORKLOAD CONFIGURATION
        // ----------------------------------------------------------------

        WorkloadConfiguration wrkld = new WorkloadConfiguration();
        wrkld.setBenchmarkName(plugin);
        wrkld.setXmlConfig(xmlConfig);

        // Pull in database configuration
        wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getString("type")));
        wrkld.setDriverClass(xmlConfig.getString("driver"));
        wrkld.setUrl(xmlConfig.getString("url"));
        wrkld.setUsername(xmlConfig.getString("username"));
        wrkld.setPassword(xmlConfig.getString("password"));
        wrkld.setRandomSeed(xmlConfig.getInt("randomSeed", -1));
        wrkld.setBatchSize(xmlConfig.getInt("batchsize", 128));

        wrkld.setMaxRetries(xmlConfig.getInt("retries", 3));
        wrkld.setFastBackoffSlotMillis(xmlConfig.getLong("fastBackoffSlotMillis", wrkld.getFastBackoffSlotMillis()));
        wrkld.setFastBackoffCeiling(xmlConfig.getInt("fastBackoffSlotMillis", wrkld.getFastBackoffCeiling()));
        wrkld.setBackoffSlotMillis(xmlConfig.getLong("backoffSlotMillis", wrkld.getBackoffSlotMillis()));
        wrkld.setBackoffCeiling(xmlConfig.getInt("backoffSlotMillis", wrkld.getBackoffCeiling()));

        wrkld.setNewConnectionPerTxn(xmlConfig.getBoolean("newConnectionPerTxn", false));

        int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
        terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
        wrkld.setTerminals(terminals);
        wrkld.setStartFrom(startFromId);

        if (xmlConfig.containsKey("loaderThreads")) {
            int loaderThreads = xmlConfig.getInt("loaderThreads");
            wrkld.setLoaderThreads(loaderThreads);
        }

        String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
        wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));

        double scaleFactor = xmlConfig.getDouble("scalefactor", 1.0);
        numWarehouses = (int) scaleFactor;
        wrkld.setScaleFactor(scaleFactor);

        if (totalWarehousesInCompany != 0) {
            wrkld.setTotalWarehousesInCompany(totalWarehousesInCompany);
        } else {
            wrkld.setTotalWarehousesInCompany(numWarehouses);
        }

        wrkld.setDataDir(xmlConfig.getString("datadir", "."));
        wrkld.setDDLPath(xmlConfig.getString("ddlpath", null));

        double selectivity = -1;
        try {
            selectivity = xmlConfig.getDouble("selectivity");
            wrkld.setSelectivity(selectivity);
        } catch (NoSuchElementException nse) {
            // Nothing to do here !
        }

        if (xmlConfig.containsKey("strict")) {
            // be as much strict TPC-C as possible, otherwise closer to
            // CockroachDB, YugabyteDB and TiDB implementations
            wrkld.setStrictMode(xmlConfig.getBoolean("strict"));
        }

        // ----------------------------------------------------------------
        // CREATE BENCHMARK MODULE
        // ----------------------------------------------------------------

        String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

        if (classname == null) {
            throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml", 1);
        }

        BenchmarkModule benchmark = ClassUtil.newInstance(classname, new Object[]{wrkld}, new Class<?>[]{WorkloadConfiguration.class});
        Map<String, Object> initDebug = new ListOrderedMap<>();
        initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
        initDebug.put("Configuration", configFile);
        initDebug.put("Type", wrkld.getDatabaseType());
        initDebug.put("Driver", wrkld.getDriverClass());
        initDebug.put("URL", wrkld.getUrl());
        initDebug.put("Isolation", wrkld.getIsolationString());
        initDebug.put("Batch Size", wrkld.getBatchSize());
        initDebug.put("Scale Factor", wrkld.getScaleFactor());
        initDebug.put("Terminals", wrkld.getTerminals());
        initDebug.put("New Connection Per Txn", wrkld.getNewConnectionPerTxn());

        if (selectivity != -1) {
            initDebug.put("Selectivity", selectivity);
        }

        LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
        LOG.info(SINGLE_LINE);

        // ----------------------------------------------------------------
        // LOAD TRANSACTION DESCRIPTIONS
        // ----------------------------------------------------------------
        int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
        if (numTxnTypes == 0 && targetList.length == 1) {
            //if it is a single workload run, <transactiontypes /> w/o attribute is used
            pluginTest = "[not(@bench)]";
            numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
        }


        List<TransactionType> ttypes = new ArrayList<>();
        ttypes.add(TransactionType.INVALID);
        int txnIdOffset = lastTxnId;
        for (int i = 1; i <= numTxnTypes; i++) {
            String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
            String txnName = xmlConfig.getString(key + "/name");
            if (txnName.equals("NewOrder")) {
                newOrderTxnId = i + txnIdOffset;
            }

            // Get ID if specified; else increment from last one.
            int txnId = i;
            if (xmlConfig.containsKey(key + "/id")) {
                txnId = xmlConfig.getInt(key + "/id");
            }

            long preExecutionWait = 0;
            if (xmlConfig.containsKey(key + "/preExecutionWait")) {
                preExecutionWait = xmlConfig.getLong(key + "/preExecutionWait");
            }

            long postExecutionWait = 0;
            if (xmlConfig.containsKey(key + "/postExecutionWait")) {
                postExecutionWait = xmlConfig.getLong(key + "/postExecutionWait");
            }

            TransactionType tmpType = benchmark.initTransactionType(txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);

            // Keep a reference for filtering
            activeTXTypes.add(tmpType);

            // Add a ref for the active TTypes in this benchmark
            ttypes.add(tmpType);
            lastTxnId = i;
        }

        // Wrap the list of transactions and save them
        TransactionTypes tt = new TransactionTypes(ttypes);
        wrkld.setTransTypes(tt);
        LOG.debug("Using the following transaction types: {}", tt);

        // Read in the groupings of transactions (if any) defined for this
        // benchmark
        int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
        LOG.debug("Num groupings: {}", numGroupings);
        for (int i = 1; i < numGroupings + 1; i++) {
            String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

            // Get the name for the grouping and make sure it's valid.
            String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
            if (!groupingName.matches("^[a-z]\\w*$")) {
                LOG.error(String.format("Grouping name \"%s\" is invalid." + " Must begin with a letter and contain only" + " alphanumeric characters.", groupingName));
                System.exit(-1);
            } else if (groupingName.equals("all")) {
                LOG.error("Grouping name \"all\" is reserved." + " Please pick a different name.");
                System.exit(-1);
            }

            // Get the weights for this grouping and make sure that there
            // is an appropriate number of them.
            List<String> groupingWeights = Arrays.asList(xmlConfig.getString(key + "/weights").split("\\s*,\\s*"));
            if (groupingWeights.size() != numTxnTypes) {
                LOG.error(String.format("Grouping \"%s\" has %d weights," + " but there are %d transactions in this" + " benchmark.", groupingName, groupingWeights.size(), numTxnTypes));
                System.exit(-1);
            }

            LOG.debug("Creating grouping with name, weights: {}, {}", groupingName, groupingWeights);
        }


        // ----------------------------------------------------------------
        // WORKLOAD CONFIGURATION
        // ----------------------------------------------------------------

        int size = xmlConfig.configurationsAt("/works/work").size();
        if (size != 1) {
            LOG.error("Only one work is allowed");
            System.exit(-1);
        }

        for (int i = 1; i < size + 1; i++) {
            final HierarchicalConfiguration<ImmutableNode> work = xmlConfig.configurationAt("works/work[" + i + "]");
            List<String> weight_strings;

            // use a workaround if there are multiple workloads or single
            // attributed workload
            if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
            } else {
                weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
            }

            Phase.Arrival arrival = Phase.Arrival.REGULAR;
            String arrive = work.getString("@arrival", "regular");
            if (arrive.equalsIgnoreCase("POISSON")) {
                arrival = Phase.Arrival.POISSON;
            }

            int activeTerminals;
            activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
            activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);

            if (activeTerminals > terminals) {
                LOG.error(String.format("Configuration error in work %d: " + "Number of active terminals is bigger than the total number of terminals", i));
                System.exit(-1);
            }

            time = work.getInt("/time", 0);
            int warmup = work.getInt("/warmup", 0);
            boolean timed = (time > 0);
            if (!timed) {
                LOG.error("Must provide positive time bound executions.");
                System.exit(-1);
            }
            if (warmup < 0) {
                LOG.error("Must provide non-negative time bound for" + " warmup.");
                System.exit(-1);
            }

            wrkld.setWarmupTime(warmup);
            ArrayList<Double> weights = new ArrayList<>();

            double totalWeight = 0;

            for (String weightString : weight_strings) {
                double weight = Double.parseDouble(weightString);
                totalWeight += weight;
                weights.add(weight);
            }

            long roundedWeight = Math.round(totalWeight);

            if (roundedWeight != 100) {
                LOG.warn("rounded weight [{}] does not equal 100.  Original weight is [{}]", roundedWeight, totalWeight);
            }

            wrkld.setPhase(i, time, warmup, weights, timed, activeTerminals, arrival);
        }

        Phase phase = wrkld.getPhase();
        if (phase.getWeightCount() != numTxnTypes) {
            LOG.error(String.format("Configuration files is inconsistent: contains %d weights but you defined %d transaction types", phase.getWeightCount(), numTxnTypes));
            System.exit(-1);
        }

        // Generate the dialect map
        wrkld.init();

        int monitoringPort = xmlConfig.getInt("monitoringPort", 0);
        if (monitoringPort > 0) {
            LOG.info("Start prometeus metric collector on port {}", monitoringPort);
            PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            HTTPServer server = new HTTPServer(
                    new InetSocketAddress(monitoringPort),
                    prometheusRegistry.getPrometheusRegistry(),
                    true);
            LOG.info("Started {}", server);
            Metrics.addRegistry(prometheusRegistry);
            String instance = xmlConfig.getString("monitoringName", "benchbase");
            Metrics.globalRegistry.config().commonTags(Tags.of("instance", instance));
        }

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            try {
                LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                runCreator(benchmark);
                LOG.info("Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
            } catch (Throwable ex) {
                LOG.error("Unexpected error when creating benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping creating benchmark database tables");
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
            try {
                LOG.info("Clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
                benchmark.refreshCatalog();
                benchmark.clearDatabase();
                benchmark.refreshCatalog();
                LOG.info("Finished clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
            } catch (Throwable ex) {
                LOG.error("Unexpected error when clearing benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping clearing benchmark database tables");
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            try {
                LOG.info("Loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
                runLoader(benchmark);
                LOG.info("Finished loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
            } catch (Throwable ex) {
                LOG.error("Unexpected error when loading benchmark database records.", ex);
                System.exit(1);
            }

        } else {
            LOG.debug("Skipping loading benchmark database records");
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            try {
                Results r = runWorkload(benchmark, intervalMonitor);
                printToplineResults(r);
                writeOutputs(r, activeTXTypes, argsLine, xmlConfig);
                writeHistograms(r);

                if (argsLine.hasOption("json-histograms")) {
                    String histogram_json = writeJSONHistograms(r);
                    String fileName = argsLine.getOptionValue("json-histograms");
                    FileUtil.writeStringToFile(new File(fileName), histogram_json);
                    LOG.info("Histograms JSON Data: " + fileName);
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when executing benchmarks.", ex);
                System.exit(1);
            }

        } else {
            LOG.info("Skipping benchmark workload execution");
        }

        YdbDriver.deregister();
    }

    private static Options buildOptions(XMLConfiguration pluginConfig) {
        Options options = new Options();
        options.addOption("b", "bench", true, "[required] Benchmark class. Currently supported: " + pluginConfig.getList("/plugin//@name"));
        options.addOption("c", "config", true, "[required] Workload configuration file");
        options.addOption(null, "create", true, "Initialize the database for this benchmark");
        options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
        options.addOption(null, "load", true, "Load data using the benchmark's data loader");
        options.addOption(null, "execute", true, "Execute the benchmark workload");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
        options.addOption("sf", "start-from-id", true, "Start from a specific scale instance id");
        options.addOption("totalwh", "total-warehouses", true, "Total warehouses in company");
        options.addOption("nob", "no-bulk-load", false, "Don't use bulk upsert to load the data");
        options.addOption("rt", "real-threads", false, "Use real threads");
        return options;
    }

    private static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                .configure(params.xml()
                        .setFileName(filename)
                        .setListDelimiterHandler(new DisabledListDelimiterHandler())
                        .setExpressionEngine(new XPathExpressionEngine()));
        return builder.getConfiguration();

    }

    private static void writeHistograms(Results r) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");

        sb.append(StringUtil.bold("Completed Transactions:")).append("\n").append(r.getSuccess()).append("\n\n");

        sb.append(StringUtil.bold("Aborted Transactions:")).append("\n").append(r.getAbort()).append("\n\n");

        sb.append(StringUtil.bold("Rejected Transactions (Server Retry):")).append("\n").append(r.getRetry()).append("\n\n");

        sb.append(StringUtil.bold("Rejected Transactions (Retry Different):")).append("\n").append(r.getRetryDifferent()).append("\n\n");

        sb.append(StringUtil.bold("Unexpected SQL Errors:")).append("\n").append(r.getError()).append("\n\n");

        sb.append(StringUtil.bold("Unknown Status Transactions:")).append("\n").append(r.getUnknown()).append("\n\n");

        if (!r.getAbortMessages().isEmpty()) {
            sb.append("\n\n").append(StringUtil.bold("User Aborts:")).append("\n").append(r.getAbortMessages());
        }

        LOG.info(SINGLE_LINE);
        LOG.info("Workload Histograms:\n{}", sb);
        LOG.info(SINGLE_LINE);
    }

    private static String writeJSONHistograms(Results r) {
        Map<String, JSONSerializable> map = new HashMap<>();
        map.put("completed", r.getSuccess());
        map.put("aborted", r.getAbort());
        map.put("rejected", r.getRetry());
        map.put("unexpected", r.getError());
        return JSONUtil.toJSONString(map);
    }

    /**
     * Write out the results for a benchmark run to a bunch of files
     *
     * @param r
     * @param activeTXTypes
     * @param argsLine
     * @param xmlConfig
     * @throws Exception
     */
    private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine, XMLConfiguration xmlConfig) throws Exception {

        // If an output directory is used, store the information
        String outputDirectory = "results";

        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }


        FileUtil.makeDirIfNotExists(outputDirectory);
        ResultWriter rw = new ResultWriter(r, xmlConfig, argsLine);

        String name = StringUtils.join(StringUtils.split(argsLine.getOptionValue("b"), ','), '-');

        String baseFileName = name + "_" + TimeUtil.getCurrentTimeString();

        String rawFileName = baseFileName + ".raw.json";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, rawFileName))) {
            LOG.info("Output Raw data into file: {}", rawFileName);
            rw.writeRaw(activeTXTypes, ps);
        }

        String summaryFileName = baseFileName + ".summary.json";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, summaryFileName))) {
            LOG.info("Output summary data into file: {}", summaryFileName);
            rw.writeSummary(ps);
        }

        String paramsFileName = baseFileName + ".params.json";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, paramsFileName))) {
            LOG.info("Output DBMS parameters into file: {}", paramsFileName);
            rw.writeParams(ps);
        }

        if (rw.hasMetrics()) {
            String metricsFileName = baseFileName + ".metrics.json";
            try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, metricsFileName))) {
                LOG.info("Output DBMS metrics into file: {}", metricsFileName);
                rw.writeMetrics(ps);
            }
        }

        String configFileName = baseFileName + ".config.xml";
        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, configFileName))) {
            LOG.info("Output benchmark config into file: {}", configFileName);
            rw.writeConfig(ps);
        }
    }

    private static void runCreator(BenchmarkModule bench) throws SQLException, IOException {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }

    private static void runLoader(BenchmarkModule bench) throws SQLException, InterruptedException {
        LOG.debug(String.format("Loading %s Database", bench));
        bench.loadDatabase();
    }

    private static Results runWorkload(BenchmarkModule benchmark, int intervalMonitor) throws IOException {
        List<Worker<?>> workers = new ArrayList<>();
        List<WorkloadConfiguration> workConfs = new ArrayList<>();

        LOG.info("Creating {} virtual terminals...", benchmark.getWorkloadConfiguration().getTerminals());
        workers.addAll(benchmark.makeWorkers());

        LOG.info(String.format("Launching the %s Benchmark", benchmark.getBenchmarkName().toUpperCase()));
        WorkloadConfiguration workConf = benchmark.getWorkloadConfiguration();

        Results r = ThreadBench.runBenchmark(workers, workConf, intervalMonitor, useRealThreads);
        return r;
    }

    private static void printToplineResults(Results r) {
        long numNewOrderTransactions = r.getStats().getSuccessCount(newOrderTxnId);

        double tpmc = 1.0 * numNewOrderTransactions * 60 / time;
        double efficiency = 1.0 * tpmc * 100 / numWarehouses / 12.86;
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        String resultOut = "\n"
                + "================RESULTS================\n"
                + String.format("%18s | %18d\n", "Time, s", time)
                + String.format("%18s | %18d\n", "NewOrders", numNewOrderTransactions)
                + String.format("%18s | %18.2f\n", "TPM-C", tpmc)
                + String.format("%18s | %17.2f%%\n", "Efficiency", efficiency)
                + String.format("reqs/s: %s\n", r);

        LOG.info(SINGLE_LINE);
        LOG.info(resultOut);
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("benchbase", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     *
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '{}'. Checking whether set to true", key);
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null && val.equalsIgnoreCase("true"));
        }
        return (false);
    }
}
