#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 8 ]]; then
    echo "usage: $0 <broker> <port> <qos> <subscribers> <payload> <messages> <warmup-messages> <output-dir>" >&2
    exit 2
fi

broker=$1
port=$2
qos=$3
subscribers=$4
payload=$5
messages=$6
warmup_messages=$7
output_dir=$8

root=${MQTTZERO_DEV_ROOT:-/home/ubuntu/mqttzero-dev}
runtime_dir=${MQTTZERO_BROKER_DIR:-"$root/artifacts/broker-linux-x64/current/runtime"}
emqtt_bin=${EMQTT_BENCH_BIN:-"$root/tools/emqtt-bench/0.6.4/bin/emqtt_bench"}
connect_rate=${CONNECT_RATE:-100}
subscriber_settle_seconds=${SUBSCRIBER_SETTLE_SECONDS:-1}
max_pending=${MAX_PENDING:-1024}
drain_timeout_seconds=${DRAIN_TIMEOUT_SECONDS:-180}
qoe_flush_seconds=${QOE_FLUSH_SECONDS:-15}
publish_interval_ms=${PUBLISH_INTERVAL_MS:-0}
process_sample_ms=${PROCESS_SAMPLE_MS:-100}
enable_internal_memory_probe=${ENABLE_MQTTZERO_MEMORY_PROBE:-0}
memory_probe_ms=${MEMORY_PROBE_MS:-100}
run_id=$(date +%Y%m%d-%H%M%S-%N)
topic="bench/${broker}/${qos}/${subscribers}/${payload}/${run_id}"

mkdir -p "$output_dir"
broker_log="$output_dir/broker.log"
subscriber_log="$output_dir/subscriber.log"
warmup_publish_log="$output_dir/warmup-publish.log"
publish_log="$output_dir/publish.log"
qoe_log="$output_dir/qoe.dlog"
qoe_csv="$qoe_log.csv"
qoe_dump_log="$output_dir/qoe-dump.log"
summary_json="$output_dir/summary.json"
summary_markdown="$output_dir/summary.md"
phases_json="$output_dir/phases.json"
broker_samples="$output_dir/broker-samples.csv"
subscriber_samples="$output_dir/subscriber-samples.csv"
publisher_samples="$output_dir/publisher-samples.csv"

rm -f \
    "$broker_log" \
    "$subscriber_log" \
    "$warmup_publish_log" \
    "$publish_log" \
    "$qoe_log" \
    "$qoe_csv" \
    "$qoe_dump_log" \
    "$summary_json" \
    "$summary_markdown" \
    "$phases_json" \
    "$broker_samples" \
    "$subscriber_samples" \
    "$publisher_samples"

broker_pid=""
subscriber_pid=""
publisher_pid=""
broker_sampler_pid=""
subscriber_sampler_pid=""
publisher_sampler_pid=""
broker_start_ns=0
broker_ready_ns=0
subscriber_start_ns=0
subscriber_connected_ns=0
warmup_publisher_start_ns=0
warmup_publish_start_ns=0
warmup_publish_complete_ns=0
warmup_drained_ns=0
measurement_start_ns=0
measurement_publisher_start_ns=0
publish_complete_ns=0
measurement_drained_ns=0
qoe_flush_complete_ns=0
stop_ns=0

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

write_phases() {
    python3 - "$phases_json" \
        "$broker_start_ns" \
        "$broker_ready_ns" \
        "$subscriber_start_ns" \
        "$subscriber_connected_ns" \
        "$warmup_publisher_start_ns" \
        "$warmup_publish_start_ns" \
        "$warmup_publish_complete_ns" \
        "$warmup_drained_ns" \
        "$measurement_publisher_start_ns" \
        "$measurement_start_ns" \
        "$publish_complete_ns" \
        "$measurement_drained_ns" \
        "$qoe_flush_complete_ns" \
        "$stop_ns" <<'PY'
import json
import sys
from pathlib import Path

keys = [
    "BrokerStartNs",
    "BrokerReadyNs",
    "SubscriberStartNs",
    "SubscriberConnectedNs",
    "WarmupPublisherStartNs",
    "WarmupPublishStartNs",
    "WarmupPublishCompleteNs",
    "WarmupDrainedNs",
    "MeasurementPublisherStartNs",
    "MeasurementStartNs",
    "PublishCompleteNs",
    "MeasurementDrainedNs",
    "QoeFlushCompleteNs",
    "StopNs",
]
values = {key: int(value) for key, value in zip(keys, sys.argv[2:])}
Path(sys.argv[1]).write_text(
    json.dumps(values, indent=2) + "\n",
    encoding="utf-8",
)
PY
}

stop_process() {
    local pid=$1
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        return 0
    fi

    local signal
    for signal in INT TERM KILL; do
        kill "-$signal" "$pid" 2>/dev/null || true
        local attempt
        for attempt in {1..20}; do
            if ! kill -0 "$pid" 2>/dev/null; then
                wait "$pid" 2>/dev/null || true
                return 0
            fi
            sleep 0.1
        done
    done

    wait "$pid" 2>/dev/null || true
}

stop_sampler() {
    local pid=$1
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    set +e
    stop_process "$publisher_pid"
    stop_sampler "$publisher_sampler_pid"
    stop_process "$subscriber_pid"
    stop_sampler "$subscriber_sampler_pid"
    stop_process "$broker_pid"
    stop_sampler "$broker_sampler_pid"
    write_phases
}

trap cleanup EXIT
trap 'exit 130' INT TERM

start_sampler() {
    local pid=$1
    local output=$2
    python3 "$script_dir/sample-process.py" \
        --pid "$pid" \
        --output "$output" \
        --interval-ms "$process_sample_ms" \
        >/dev/null 2>&1 &
    echo "$!"
}

last_total() {
    local file=$1
    local counter=$2
    tail -n 200 "$file" |
        tr '\r' '\n' |
        sed -nE "s/.*${counter} total=([0-9]+).*/\1/p" |
        tail -n 1
}

wait_for_total() {
    local file=$1
    local counter=$2
    local target=$3
    local description=$4
    local deadline=$((SECONDS + drain_timeout_seconds))
    local value=""
    while ((SECONDS < deadline)); do
        value=$(last_total "$file" "$counter")
        if [[ -n "$value" ]] && ((value >= target)); then
            echo "$value"
            return 0
        fi
        sleep 0.25
    done
    echo "timed out waiting for $description: target=$target, value=${value:-none}" >&2
    tail -n 80 "$file" >&2 || true
    return 1
}

wait_for_marker() {
    local file=$1
    local marker=$2
    local pid=$3
    local description=$4
    local deadline=$((SECONDS + 60))
    while ((SECONDS < deadline)); do
        if grep -Fq "$marker" "$file"; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$description exited before marker '$marker'" >&2
            cat "$file" >&2
            return 1
        fi
        sleep 0.05
    done
    echo "timed out waiting for $description marker '$marker'" >&2
    cat "$file" >&2
    return 1
}

wait_for_ready() {
    local deadline=$((SECONDS + 60))
    while ((SECONDS < deadline)); do
        if grep -q '^READY$' "$broker_log"; then
            return 0
        fi
        if ! kill -0 "$broker_pid" 2>/dev/null; then
            echo "broker exited before becoming ready" >&2
            cat "$broker_log" >&2
            return 1
        fi
        sleep 0.1
    done
    echo "timed out waiting for broker readiness" >&2
    cat "$broker_log" >&2
    return 1
}

wait_for_publisher() {
    local pid=$1
    local log=$2
    local description=$3
    if ! wait "$pid"; then
        echo "$description failed" >&2
        cat "$log" >&2
        return 1
    fi
}

run_publisher() {
    local output_log=$1
    local message_count=$2
    local interval_ms=$3
    local start_variable=$4
    local complete_variable=$5

    local publisher_start_ns
    publisher_start_ns=$(date +%s%N)
    if [[ "$start_variable" == "warmup" ]]; then
        warmup_publisher_start_ns=$publisher_start_ns
    else
        measurement_publisher_start_ns=$publisher_start_ns
    fi

    "$emqtt_bin" pub \
        -h 127.0.0.1 \
        -p "$port" \
        -V 5 \
        -c 1 \
        -t "$topic" \
        -q "$qos" \
        -s "$payload" \
        -L "$message_count" \
        -F "$max_pending" \
        -I "$interval_ms" \
        --payload-hdrs ts \
        -w \
        >"$output_log" 2>&1 &
    publisher_pid=$!
    publisher_sampler_pid=$(start_sampler "$publisher_pid" "$publisher_samples")

    wait_for_marker \
        "$output_log" \
        "start publishing" \
        "$publisher_pid" \
        "publisher"
    local publish_start_ns
    publish_start_ns=$(date +%s%N)
    if [[ "$start_variable" == "warmup" ]]; then
        warmup_publish_start_ns=$publish_start_ns
    else
        measurement_start_ns=$publish_start_ns
    fi

    wait_for_publisher "$publisher_pid" "$output_log" "publisher"
    local publish_complete=$publish_start_ns
    publish_complete=$(date +%s%N)
    if [[ "$complete_variable" == "warmup" ]]; then
        warmup_publish_complete_ns=$publish_complete
    else
        publish_complete_ns=$publish_complete
    fi
    publisher_pid=""
    stop_sampler "$publisher_sampler_pid"
    publisher_sampler_pid=""
}

broker_start_ns=$(date +%s%N)
broker_arguments=(
    serve
    --broker "$broker"
    --port "$port"
)
if [[ "$broker" == "mqttzero" && "$enable_internal_memory_probe" == "1" ]]; then
    broker_arguments+=(--memory-probe-ms "$memory_probe_ms")
fi

cd "$runtime_dir"
if [[ -x ./MqttZero.BrokerBenchmarks ]]; then
    ./MqttZero.BrokerBenchmarks "${broker_arguments[@]}" \
        >"$broker_log" 2>&1 &
else
    dotnet ./MqttZero.BrokerBenchmarks.dll "${broker_arguments[@]}" \
        >"$broker_log" 2>&1 &
fi
broker_pid=$!
broker_sampler_pid=$(start_sampler "$broker_pid" "$broker_samples")
wait_for_ready
broker_ready_ns=$(date +%s%N)

subscriber_start_ns=$(date +%s%N)
"$emqtt_bin" sub \
    -h 127.0.0.1 \
    -p "$port" \
    -V 5 \
    -A true \
    -c "$subscribers" \
    -R "$connect_rate" \
    -t "$topic" \
    -q "$qos" \
    --payload-hdrs ts \
    -Q true \
    --qoelog "$qoe_log" \
    --shortids \
    >"$subscriber_log" 2>&1 &
subscriber_pid=$!
subscriber_sampler_pid=$(start_sampler "$subscriber_pid" "$subscriber_samples")

wait_for_total \
    "$subscriber_log" \
    "sub" \
    "$subscribers" \
    "subscriber subscriptions" >/dev/null
sleep "$subscriber_settle_seconds"
subscriber_connected_ns=$(date +%s%N)

if ((warmup_messages > 0)); then
    run_publisher \
        "$warmup_publish_log" \
        "$warmup_messages" \
        "$publish_interval_ms" \
        warmup \
        warmup
    wait_for_total \
        "$subscriber_log" \
        "recv" \
        "$((warmup_messages * subscribers))" \
        "warm-up drain" >/dev/null
    warmup_drained_ns=$(date +%s%N)
    sleep "$qoe_flush_seconds"
fi

received_before=$(last_total "$subscriber_log" "recv")
received_before=${received_before:-0}
run_publisher \
    "$publish_log" \
    "$messages" \
    "$publish_interval_ms" \
    measurement \
    measurement
measurement_started_ms=$((measurement_start_ns / 1000000))
elapsed_seconds=$(awk -v start="$measurement_start_ns" -v end="$publish_complete_ns" 'BEGIN { printf "%.9f", (end - start) / 1000000000 }')
expected_deliveries=$((messages * subscribers))
received_after=$(wait_for_total \
    "$subscriber_log" \
    "recv" \
    "$((received_before + expected_deliveries))" \
    "measurement drain")
measurement_drained_ns=$(date +%s%N)
delivery_elapsed_seconds=$(awk -v start="$measurement_start_ns" -v end="$measurement_drained_ns" 'BEGIN { printf "%.9f", (end - start) / 1000000000 }')

sleep "$qoe_flush_seconds"
qoe_flush_complete_ns=$(date +%s%N)

stop_process "$subscriber_pid"
subscriber_pid=""
stop_sampler "$subscriber_sampler_pid"
subscriber_sampler_pid=""

"$emqtt_bin" sub \
    -t dump \
    -Q dump \
    --qoelog "$qoe_log" \
    >"$qoe_dump_log" 2>&1

stop_process "$broker_pid"
broker_pid=""
stop_sampler "$broker_sampler_pid"
broker_sampler_pid=""
stop_ns=$(date +%s%N)
write_phases

python3 "$script_dir/summarize.py" \
    --csv "$qoe_csv" \
    --output "$summary_json" \
    --markdown-output "$summary_markdown" \
    --broker "$broker" \
    --run-id "$run_id" \
    --qos "$qos" \
    --subscribers "$subscribers" \
    --payload "$payload" \
    --messages "$messages" \
    --warmup-messages "$warmup_messages" \
    --publish-interval-ms "$publish_interval_ms" \
    --started-ms "$measurement_started_ms" \
    --elapsed-seconds "$elapsed_seconds" \
    --delivery-elapsed-seconds "$delivery_elapsed_seconds" \
    --received-before "$received_before" \
    --received-after "$received_after" \
    --expected-deliveries "$expected_deliveries" \
    --phases "$phases_json" \
    --broker-samples "$broker_samples" \
    --subscriber-samples "$subscriber_samples" \
    --publisher-samples "$publisher_samples" \
    --broker-log "$broker_log"

echo "$summary_json"
