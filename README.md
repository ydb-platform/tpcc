# Overview

This is a fork of [BenchBase](https://github.com/cmu-db/benchbase), with the following enhancements:
1. Added support for [YDB](https://ydb.tech) (TPC-C only).
2. Fixed some performance issues in the original benchbase to speed up the benchmark.
3. To address issues with running high number of warehouses, we added support for virtual threads (requires Java >= 19, we have tested on Java 20).
4. Significantly reduced the memory footprint of the benchmark.

Short decriptions of branches:
* main - the BenchBase with YDB support.
* vanilla - the original BenchBase with performance enhancements.
* postgres - vanilla with c3p0 as a connection pool.

## Hardware requirements

Minumum requirements for running the benchmark against YDB:
* 2 cores and 4 GB RAM (for 100 warehouses)
* 4 cores and 6 GB RAM (for 1000 warehouses)

Above 1000 warehouses, the memory and CPU consumption grow linearly, you need:
* 1 core per 1000 warehouses
* 6 MB RAM per warehouse

E.g. to run 10000 warehouses you need to have at least 10 cores and 64 GB RAM. However, Instead of running 10000 warehouses on a single instance (and machine), we recommend to run at most 5000 warehouses per instance (preferably on separate machines).

To reduce memory consumption, make sure you don't use huge pages or transparent huge pages.

# TPC-C benchmark for YDB

## How to build

Prebuilt packages:
* [benchbase-ydb.tgz](https://storage.yandexcloud.net/ydb-benchmark-builds/benchbase-ydb.tgz)

To build the benchmark you need Java >= 19 (we tested on Java 20). This project depends on the development version of [ydb-jdbc-driver](https://github.com/ydb-platform/ydb-jdbc-driver):
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