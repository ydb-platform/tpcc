#!/bin/bash

MEMORY='1G'

args=()

while [[ "$#" > 0 ]]; do case $1 in
    --memory)
        memory=$2
        shift;;
    *)
        args+=("$1")
        ;;
esac; shift; done

if [[ -z $memory ]]; then
    memory=$MEMORY
fi

if command -v java >/dev/null; then
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ $java_version = "17"* ]]; then
        java="java"
    fi
fi

if [[ -z $java ]]; then
    if [[ -e "/usr/lib/jvm/java-17-openjdk-amd64/bin/java" ]]; then
        java="/usr/lib/jvm/java-17-openjdk-amd64/bin/java"
    elif [[ -e "/usr/lib/jvm/java-17/bin/java" ]]; then
        java="/usr/lib/jvm/java-17/bin/java"
    else
        echo "Java 17 is required to run this script."
        exit 1
    fi
fi

$java -Xmx$memory -jar benchbase.jar -b tpcc "${args[@]}"
