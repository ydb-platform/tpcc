# TPC-C benchmark for YDB

This is a fork of [BenchBase](https://github.com/cmu-db/benchbase), with the following enhancements:
1. Added support for [YDB](https://ydb.tech).
2. Fixed some performance issues in the original benchbase to speed up the benchmark.

## How to build

To build the benchmark you need Java-17. This project depends on the development version of [ydb-jdbc-driver](https://github.com/ydb-platform/ydb-jdbc-driver):
1. Clone the repository.
2. Build and install ydb-jdbc-driver:
```
mvn clean install -DskipTests
```

Next, to build the benchbase-ydb package, execute the following command:
```
./mvnw clean package -P ydb -DskipTests
```

## How to run

The simplest way is to use helper scripts from [benchhelpers](https://github.com/ydb-platform/benchhelpers/) located in `tpcc/ydb` folder. You can find the full instructions [here](https://github.com/ydb-platform/benchhelpers/blob/main/tpcc/ydb/README.md).
