# AstraLog Control Center Adapter

This crate is the local read-only Rust adapter for the AstraLog Control Center UI. It reads live cluster metadata through `astralog-client` and exposes a small HTTP API for a local Vite frontend.

It does not call CreateTopic, DeleteTopic, Produce, reassignment, group mutation, or any other mutating broker API. It does not report synthetic live latency or throughput metrics.

## Run

```text
cargo run -p astralog-dashboard -- --bootstrap 127.0.0.1:9092 --bind 127.0.0.1:8787
```

Supported flags:

* `--bootstrap <host:port>` selects the AstraLog bootstrap broker.
* `--bind <ip:port>` selects the adapter HTTP listener. Only loopback IP addresses are accepted.
* `--poll-interval-ms <ms>` controls the metadata refresh cadence. The minimum is 100 ms.
* `--poll-timeout-ms <ms>` places a hard bound on each metadata poll. The accepted range is 5 through 300000 ms.
* `--vite-origin <origin>` adds an allowed local Vite origin. Repeat it for multiple ports. Custom values replace the default localhost origins.
* `--help` prints the CLI reference.

The default Vite origins are `http://127.0.0.1:5173` and `http://localhost:5173`. Remote origins are rejected.

## HTTP API

`GET /api/overview` returns schema version 1 with adapter connection state and the latest real AstraLog metadata snapshot. The cluster payload includes brokers, topics, partitions, partition leaders, leader epochs, and replica broker IDs.

Adapter state is `connected` after a successful poll, `disconnected` when no live snapshot has ever been obtained, and `unavailable` when a later poll fails after a prior success. In the unavailable state, the last successful cluster snapshot remains present and its `last_success_at_ms` timestamp makes the stale boundary explicit.

`GET /api/health` reports whether the adapter process is serving and the current metadata connection state. Adapter HTTP health is separate from AstraLog cluster reachability.
