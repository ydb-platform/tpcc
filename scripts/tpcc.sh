#!/bin/bash

memory='1G'

args=()

while [[ "$#" > 0 ]]; do case $1 in
    --memory)
        memory=$2
        shift;;
    *)
        args+=("$1")
        ;;
esac; shift; done

min_java_version=20

if command -v java >/dev/null; then
    java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')

    if [[ $java_version -ge $min_java_version ]]; then
        java="java"
    fi
fi

if [[ -z "$java" ]]; then
    for d in /usr/lib/jvm/*${min_java_version}*; do
        java_check="$d/bin/java"
        if [[ -x $java_check ]]; then
            java_version=$($java_check -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
            if [[ $java_version -ge $min_java_version ]]; then
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
