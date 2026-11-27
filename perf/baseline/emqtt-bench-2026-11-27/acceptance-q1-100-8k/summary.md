# eMQTT-Bench throughput matrix

Development evidence on the current host; not a capacity claim.

| QoS | Subs | Payload | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | MQTTZero p99 ms | MQTTnet p99 ms | P99 ratio | Broker CPU % (zero/net) | Broker peak RSS MB (zero/net) | Passed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 100 | 8192 | 16988.9 | 17064.1 | 0.996 | 3821.0 | 3605.0 | 1.060 | 55.7/44.0 | 145.0/158.1 | False |
