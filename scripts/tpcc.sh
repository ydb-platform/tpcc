#!/bin/bash

if command -v java >/dev/null; then
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ $java_version = "17"* ]]; then
        java="java"
    fi
fi

if [[ -z $java ]]; then
    if [[ -e "/usr/lib/jvm/java-17-openjdk-amd64/bin/java" ]]; then
        java="/usr/lib/jvm/java-17-openjdk-amd64/bin/java"
    else
        echo "Java 17 is required to run this script."
        exit 1
    fi
fi

$java -jar benchbase.jar -b tpcc "$@"
