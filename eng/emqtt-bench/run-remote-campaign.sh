#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
    echo "usage: $0 <throughput|latency> <output-root> <qos> <subscribers> <payload>" >&2
    exit 2
fi

mode=$1
output_root=$2
qos=$3
subscribers=$4
payload=$5
smoke_messages=${SMOKE_MESSAGES:-0}
repetitions=${REPETITIONS:-2}
start_port=${START_PORT:-19000}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if ((smoke_messages > 0)); then
    interval_ms=0
    if [[ "$mode" == "latency" ]]; then
        interval_ms=20
    elif [[ "$qos" == "0" ]]; then
        interval_ms=2
    fi

    port=$start_port
    for broker in mqttzero mqttnet; do
        PUBLISH_INTERVAL_MS="$interval_ms" \
        bash "$script_dir/run-cell.sh" \
            "$broker" \
            "$port" \
            "$qos" \
            "$subscribers" \
            "$payload" \
            "$smoke_messages" \
            0 \
            "$output_root/smoke/$broker"
        port=$((port + 1))
    done
    exit 0
fi

REPETITIONS="$repetitions" \
START_PORT="$start_port" \
bash "$script_dir/run-matrix.sh" \
    "$mode" \
    "$output_root" \
    "$qos" \
    "$subscribers" \
    "$payload"
