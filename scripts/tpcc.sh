#!/bin/bash

memory='1G'

args=()

while [[ "$#" > 0 ]]; do case $1 in
    --memory)
        memory=$2
        shift;;
    --virtual-threads)
        vthreads=1
        args+=("$1")
        shift;;
    *)
        args+=("$1")
        ;;
esac; shift; done

if [[ -n $vthreads ]]; then
    java_version=20
else
    java_version=17
fi

if command -v java >/dev/null; then
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')

    if [[ $java_version = "$java_version"* ]]; then
        java="java"
    fi
fi

if [[ -z "$java" ]]; then
    # check well known paths
    if [[ -n $vthreads ]]; then
        if [[ -x "/usr/lib/jvm/bellsoft-java20-runtime-amd64/bin/java" ]]; then
            java="/usr/lib/jvm/bellsoft-java20-runtime-amd64/bin/java"
        elif [[ -x "/usr/lib/jvm/java-17/bin/java" ]]; then
            java="/usr/lib/jvm/java-17/bin/java"
        fi
    else
        if [[ -x "/usr/lib/jvm/java-17-openjdk-amd64/bin/java" ]]; then
            java="/usr/lib/jvm/java-17-openjdk-amd64/bin/java"
        elif [[ -x "/usr/lib/jvm/java-17/bin/java" ]]; then
            java="/usr/lib/jvm/java-17/bin/java"
        fi
    fi
fi

if [[ -z "$java" ]]; then
    # try to find any appropriate java version
    for d in /usr/lib/jvm/*; do
        java_check="$d/bin/java"
        if [[ -x $java_check ]]; then
            java_version=$($java_check -version 2>&1 | awk -F '"' '/version/ {print $2}')
            if [[ $java_version = "$java_version"* ]]; then
                java=$java_check
                break
            fi
        fi
    done
fi

if [[ -z "$java" ]]; then
    echo "No Java $java_version found"
    exit 1
fi

$java --enable-preview -Xmx$memory -jar benchbase.jar -b tpcc "${args[@]}"
