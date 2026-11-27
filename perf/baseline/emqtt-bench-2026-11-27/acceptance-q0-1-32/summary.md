# eMQTT-Bench throughput matrix

Development evidence on the current host; not a capacity claim.

| QoS | Subs | Payload | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | MQTTZero p99 ms | MQTTnet p99 ms | P99 ratio | Broker CPU % (zero/net) | Broker peak RSS MB (zero/net) | Passed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 1 | 32 | 462.2 | 463.4 | 0.997 | 1.0 | 1.0 | 1.000 | 18.9/18.3 | 72.5/68.8 | False |
