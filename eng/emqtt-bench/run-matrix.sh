#!/usr/bin/env bash
set -euo pipefail

mode=${1:-throughput}
output_root=${2:-"artifacts/emqtt-bench/$mode"}
qos_values=${3:-0,1,2}
subscriber_values=${4:-1,100,500}
payload_values=${5:-32,8192,65536}
repetitions=${REPETITIONS:-3}
start_port=${START_PORT:-19000}
latency_message_count=${LATENCY_MESSAGE_COUNT:-500}
latency_warmup_messages=${LATENCY_WARMUP_MESSAGES:-50}
latency_publish_interval_ms=${LATENCY_PUBLISH_INTERVAL_MS:-20}

if [[ "$mode" != "throughput" && "$mode" != "latency" ]]; then
    echo "usage: $0 <throughput|latency> [output-root] [qos-list] [subscriber-list] [payload-list]" >&2
    exit 2
fi
if ((repetitions < 1)); then
    echo "REPETITIONS must be at least 1" >&2
    exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$output_root"
runs_file="$output_root/runs.tsv"
printf 'mode\tqos\tsubscribers\tpayload\tmessage_count\twarmup_messages\trepetition\tbroker\tport\tattempt\tsummary\n' >"$runs_file"

message_count_for() {
    local payload=$1
    if [[ "$mode" == "latency" ]]; then
        echo "$latency_message_count"
        return
    fi
    case "$payload" in
        32) echo 5000 ;;
        8192) echo 1000 ;;
        65536) echo 100 ;;
        *)
            echo "unsupported payload $payload" >&2
            return 1
            ;;
    esac
}

warmup_count_for() {
    local messages=$1
    if [[ "$mode" == "latency" ]]; then
        echo "$latency_warmup_messages"
        return
    fi
    local warmup=$((messages / 10))
    if ((warmup < 5)); then
        warmup=5
    elif ((warmup > 100)); then
        warmup=100
    fi
    echo "$warmup"
}

run_is_valid() {
    local summary=$1
    python3 - "$summary" "$mode" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
valid = (
    result.get("latencyValid", False)
    if sys.argv[2] == "latency"
    else result.get("throughputValid", False)
)
raise SystemExit(0 if valid else 1)
PY
}

next_port=$start_port
for qos in ${qos_values//,/ }; do
    for subscribers in ${subscriber_values//,/ }; do
        for payload in ${payload_values//,/ }; do
            messages=$(message_count_for "$payload")
            warmup_messages=$(warmup_count_for "$messages")
            cell_name="qos${qos}-subs${subscribers}-payload${payload}"
            for ((repetition = 1; repetition <= repetitions; repetition++)); do
                if ((repetition % 2 == 1)); then
                    brokers=(mqttzero mqttnet)
                else
                    brokers=(mqttnet mqttzero)
                fi

                for broker in "${brokers[@]}"; do
                    accepted=false
                    for attempt in 1 2; do
                        next_port=$((next_port + 1))
                        run_dir="$output_root/$cell_name/repetition-$repetition/$broker/attempt-$attempt"
                        mkdir -p "$run_dir"
                        publish_interval=0
                        if [[ "$mode" == "latency" ]]; then
                            publish_interval=$latency_publish_interval_ms
                        elif [[ "$qos" == "0" ]]; then
                            # eMQTT-Bench 0.6.4 QoS 0 loses messages at -I 0/1.
                            publish_interval=2
                        fi

                        echo "mode=$mode cell=$cell_name repetition=$repetition broker=$broker port=$next_port attempt=$attempt"
                        if PUBLISH_INTERVAL_MS="$publish_interval" \
                            "$script_dir/run-cell.sh" \
                            "$broker" \
                            "$next_port" \
                            "$qos" \
                            "$subscribers" \
                            "$payload" \
                            "$messages" \
                            "$warmup_messages" \
                            "$run_dir" >"$run_dir/run.log" 2>&1; then
                            if run_is_valid "$run_dir/summary.json"; then
                                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                                    "$mode" \
                                    "$qos" \
                                    "$subscribers" \
                                    "$payload" \
                                    "$messages" \
                                    "$warmup_messages" \
                                    "$repetition" \
                                    "$broker" \
                                    "$next_port" \
                                    "$attempt" \
                                    "$run_dir/summary.json" >>"$runs_file"
                                accepted=true
                                break
                            fi
                        fi
                    done

                    if [[ "$accepted" != "true" ]]; then
                        echo "cell=$cell_name repetition=$repetition broker=$broker failed twice" >&2
                        exit 1
                    fi
                done
            done
        done
    done
done

python3 "$script_dir/summarize-matrix.py" \
    --runs "$runs_file" \
    --output "$output_root/summary.json" \
    --markdown-output "$output_root/summary.md" \
    --mode "$mode" \
    --repetitions "$repetitions"

echo "$output_root/summary.md"
