# eMQTT-Bench throughput matrix

Development evidence on the current host; not a capacity claim.

| QoS | Subs | Payload | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | MQTTZero p99 ms | MQTTnet p99 ms | P99 ratio | Broker CPU % (zero/net) | Broker peak RSS MB (zero/net) | Passed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 500 | 65536 | 3896.2 | 4300.4 | 0.906 | 11249.0 | 10372.0 | 1.085 | 27.0/24.8 | 479.7/202.6 | False |
