# eMQTT-Bench latency matrix

Development evidence on the current host; not a capacity claim.

| QoS | Subs | Payload | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | MQTTZero p99 ms | MQTTnet p99 ms | P99 ratio | Broker CPU % (zero/net) | Broker peak RSS MB (zero/net) | Passed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 8192 | 46.4 | 46.1 | 1.005 | 3.0 | 4.0 | 0.750 | 6.4/4.3 | 70.3/69.7 | True |
