# Overview

This is a fork of [BenchBase](https://github.com/cmu-db/benchbase), with the following enhancements:
1. Added support for [YDB](https://ydb.tech) (TPC-C only).
2. Fixed some performance issues in the original benchbase to speed up the benchmark.

Short decriptions of branches:
* main - the BenchBase with YDB support.
* ydb-vthreads - the BenchBase with YDB and virtual threads support (requires Java >= 19, we have tested on Java 20).
* vanilla - the original BenchBase with performance enhancements.
* vanilla-vthreads - vanilla with virtual threads support (requires Java >= 19, we have tested on Java 20).
* postgres - vanilla with c3p0 as a connection pool.

## Hardware requirements

According to the TPC-C standard, each warehouse contains 10 terminals. The original BenchBase implements each terminal as an individual thread. Thankfully, these threads are mostly in a sleeping state, resulting in minimal CPU contention (it's important to note that a high number of non-ready threads doesn't impact the Linux scheduler). However, each thread requires memory for its stack, leading to a considerable RAM consumption during the benchmark. We offer a reference table indicating the necessary amount of RAM and CPU cores for varying numbers of warehouses when executing against YDB:

| Warehouses | RAM, GB | Consumed CPU cores |
| -------: | -------: | -------: |
| 100   | 30   | 1   |
| 500   | 95   | 2   |
| 1000   | 120   | 2    |
| 2000   | 150   | 6    |
| 3000   | 162   | 10   |

With the original BenchBase's thread-per-terminal approach, we encountered limitations in running more than 3000 warehouses on a single machine with 500 GB of RAM. While it's technically feasible to execute 3000 warehouses on one machine using a single TPC-C instance, we typically cap the number of warehouses at around 500 per TPC-C instance to mitigate various types of client-side bottlenecks including garbage collection. Instead, if there are enouch CPU and RAM resources, we run multiple instances of TPC-C benchmark on a single machine. For example, to test 1000 warehouses, we run two instances of TPC-C benchmark, each simulating 500 warehouses.

Despite the low CPU consumption, there are lot of threads and we recommend to have at least 16 cores to run 1000 warehouses and higher.

With virtual threads and Java 20 we are able to run more than 3000 warehouses on a single TPC-C instance on a machine with 500 GB of RAM. However, we encountered several issues and recommend to use them only for testing purposes. For example, on our 128-core machine with hyper-threading enabled, Java's affinity setting behaves unusually by utilizing only even-numbered cores. Here is a reference table indicating the necessary amount of RAM and CPU cores for varying numbers of warehouses when executing against YDB with virtual threads:

| Warehouses | RAM, GB | Consumed CPU cores |
| -------: | -------: | -------: |
| 4000   |  338   | 4   |
| 5000   |  371   | 4   |
| 6000   |  407   | 5   |
| 7000   |  418   | 9   |
| 8000   |  423   | 15   |


# TPC-C benchmark for YDB

## How to build

Prebuilt packages:
* [benchbase-ydb.tgz](https://storage.yandexcloud.net/ydb-benchmark-builds/benchbase-ydb.tgz)
* [benchbase-ydb-vthreads.tgz](https://storage.yandexcloud.net/ydb-benchmark-builds/benchbase-ydb-vthreads.tgz)

To build the benchmark you need Java 17 (or Java >= 19 if you build version with the virtual threads). This project depends on the development version of [ydb-jdbc-driver](https://github.com/ydb-platform/ydb-jdbc-driver):
1. Clone the ydb-jdbc-driver repository.
2. Build and install the ydb-jdbc-driver:
```
mvn clean install -DskipTests
```

Next, to build the benchbase-ydb package, execute the following command:
```
./mvnw clean package -P ydb -DskipTests
```

## How to run

The simplest way is to use helper scripts from [benchhelpers](https://github.com/ydb-platform/benchhelpers/tpcc/ydb). You can find the full instruction [here](https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/ydb/README.md).

[Here](https://github.com/ydb-platform/ydb-jdbc-driver/#authentication-modes) you can find description of the authentication. Usually you will either use anonymous authentication in case of self deployed YDB, or provide a service account key file using the `saFile=file:` jdbc url parameter in [tpcc_config_template.xml](https://github.com/ydb-platform/benchhelpers/blob/108cb4ca3efc89dee7866b4bb8fca1a59ad265a8/tpcc/ydb/tpcc_config_template.xml#L7), when run managed YDB.

# TPC-C benchmark for PostgreSQL

## How to build

Checkout postgres branch and execute the following command:
```
./mvnw clean package -P postgres -DskipTests
```

## How to run

The simplest way is to use helper scripts from [benchhelpers](https://github.com/ydb-platform/benchhelpers/tpcc/postgres). You can find the full instruction [here](https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/postgres/README.md).