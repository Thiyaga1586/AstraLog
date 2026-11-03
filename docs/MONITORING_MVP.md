# AstraLog Monitoring MVP

This is a local engineering monitoring stack for the one hour observability MVP. It uses Prometheus for time series collection and Grafana for visualization. It does not invent live throughput, latency, error, or availability values.

The broker instrumentation is owned separately from this stack. The dashboard remains empty for a metric until a broker actually exports that metric from `/metrics`.

## Metric contract

The current MVP expects these Prometheus metric names:

```text
astralog_produce_records_total
astralog_produce_bytes_total
astralog_broker_requests_total{api="produce|fetch",result="ok|error"}
astralog_broker_request_duration_seconds_bucket{api="produce|fetch",le="..."}
```

The first two are monotonically increasing counters with a deliberately narrow meaning. `astralog_produce_records_total` counts records in successful acknowledged Produce partition responses. Successfully deduplicated retries are counted because the metric describes acknowledged response throughput, not unique committed append rate. It excludes `acks=0` and failed partitions. `astralog_produce_bytes_total` follows the same acknowledged response rule and counts value payload bytes only.

`astralog_broker_requests_total` counts completed public Produce and Fetch requests with bounded `api="produce|fetch"` and `result="ok|error"` labels. A request with a partition error contributes to the error result. `astralog_broker_request_duration_seconds_bucket` is the bucket series of a Prometheus histogram measured in seconds and represents server handling latency for those completed public Produce and Fetch requests.

The broker may also export `astralog_fetch_records_total` and `astralog_fetch_bytes_total` for successful Fetch return volume. They are not required by the first dashboard version and are intentionally not mixed into the Produce throughput panels.

The `api` and `result` label values must remain bounded. Do not add topic, key, client ID, request ID, error message, or other high cardinality data to these MVP metrics.

## Expected local endpoints

Run three brokers with loopback metrics listeners on the Windows host:

```text
http://127.0.0.1:9201/metrics
http://127.0.0.1:9202/metrics
http://127.0.0.1:9203/metrics
```

The metrics listener should remain loopback only. Docker Desktop provides the `host.docker.internal` hostname for containers to reach services on the Windows host, so the Prometheus targets are:

```text
host.docker.internal:9201
host.docker.internal:9202
host.docker.internal:9203
```

Prometheus adds a static `broker="1"`, `broker="2"`, or `broker="3"` label for the three targets. The native Prometheus `instance` label still contains the scrape target address.

## Start the stack on Windows with Docker Desktop

From the repository root in Windows CMD, after Docker Desktop starts:

```bat
scripts\run-monitoring.cmd
```

The launcher generates a unique cryptographically random Grafana password if
none exists, stores it in `observability\secrets\grafana_admin_password.txt`,
restricts file access to your Windows account, SYSTEM and local Administrators,
starts the containers, and resets the real persisted Grafana admin account to
that password. It verifies that anonymous API access is rejected and that the
authenticated admin account can read the dashboard API.

### Grafana login

Open `http://127.0.0.1:3000`. Use **username `admin`**. Display your own
locally stored password from Windows CMD:

```bat
type observability\secrets\grafana_admin_password.txt
```

Do not paste this password into Git, chat, screenshots or shell history.
The secret file is Git-ignored and is not part of the repository. Grafana
anonymous access, self-registration and the login-form bypass are disabled.
Its login screen is enabled, and Grafana enforces a stronger password policy.

The password lives in a Docker secret mount instead of a Compose environment
variable containing the password value. On subsequent launches the startup
script reconciles Grafana's persisted admin password with the local secret.
This is important because setting `GF_SECURITY_ADMIN_PASSWORD__FILE` alone
does **not** rotate an admin password already saved in Grafana's database.
To rotate the password, stop Grafana, delete only the local password file, and
rerun `scripts\run-monitoring.cmd`. Do **not** delete Grafana's data volume
to change a password.

The equivalent manual CMD commands, after the secret has been initialized, are:

```bat
docker compose -f observability/docker-compose.yml config
docker compose -f observability/docker-compose.yml up -d
docker compose -f observability/docker-compose.yml ps
```

Local services:

```text
Prometheus: http://127.0.0.1:9090
Grafana:    http://127.0.0.1:3000
```

Both published Docker ports are bound to host loopback. Grafana requires an
authenticated login, and public self-signup is disabled. Prometheus itself
does not have login protection in this local MVP; its web UI remains bound to
loopback. Likewise, broker `/metrics` endpoints are loopback only. Local
machine administrators and processes with sufficient privileges can still
read or access local services. Do not expose these ports on a LAN or the
internet without TLS, network controls and an appropriate authentication design.

Grafana provisions the `AstraLog Prometheus` datasource automatically and loads the `AstraLog Engineering MVP` dashboard into the `AstraLog` folder.

## Stop the stack

```bat
docker compose -f observability/docker-compose.yml down
```

To also delete the local Prometheus and Grafana Docker volumes:

```bat
docker compose -f observability/docker-compose.yml down -v
```

## Manual verification

### 1. Check each broker metrics endpoint from Windows

Run these after the broker metrics listeners are started:

```bat
curl.exe http://127.0.0.1:9201/metrics
curl.exe http://127.0.0.1:9202/metrics
curl.exe http://127.0.0.1:9203/metrics
```

The responses should contain the expected AstraLog metric names. Do not treat a successful HTTP response alone as proof that Produce or Fetch traffic has occurred.

### 2. Check Prometheus target state

Open:

```text
http://127.0.0.1:9090/targets
```

Each of the three `astralog-brokers` targets should be `UP` only when Prometheus can reach that broker metrics endpoint. A target being `UP` means the scrape succeeded. It does not prove that the broker data plane is healthy or that a partition has a leader.

### 3. Check raw metrics in Prometheus

Useful PromQL expressions:

Cluster acknowledged Produce records per second:

```promql
sum(rate(astralog_produce_records_total{job="astralog-brokers"}[1m]))
```

Acknowledged Produce records per second by scrape instance and broker label:

```promql
sum by (instance, broker) (
  rate(astralog_produce_records_total{job="astralog-brokers"}[1m])
)
```

Cluster acknowledged Produce value bytes per second:

```promql
sum(rate(astralog_produce_bytes_total{job="astralog-brokers"}[1m]))
```

Produce and Fetch requests per second:

```promql
sum by (api) (
  rate(astralog_broker_requests_total{
    job="astralog-brokers",
    api=~"produce|fetch"
  }[1m])
)
```

Produce and Fetch errors per second:

```promql
sum by (api) (
  rate(astralog_broker_requests_total{
    job="astralog-brokers",
    api=~"produce|fetch",
    result="error"
  }[1m])
)
```

Cluster request latency p50:

```promql
histogram_quantile(
  0.50,
  sum by (le, api) (
    rate(astralog_broker_request_duration_seconds_bucket{
      job="astralog-brokers",
      api=~"produce|fetch"
    }[5m])
  )
)
```

Cluster request latency p95:

```promql
histogram_quantile(
  0.95,
  sum by (le, api) (
    rate(astralog_broker_request_duration_seconds_bucket{
      job="astralog-brokers",
      api=~"produce|fetch"
    }[5m])
  )
)
```

Cluster request latency p99:

```promql
histogram_quantile(
  0.99,
  sum by (le, api) (
    rate(astralog_broker_request_duration_seconds_bucket{
      job="astralog-brokers",
      api=~"produce|fetch"
    }[5m])
  )
)
```

For an instance specific percentile, preserve both `instance` and `broker` in the histogram aggregation:

```promql
histogram_quantile(
  0.95,
  sum by (le, instance, broker, api) (
    rate(astralog_broker_request_duration_seconds_bucket{
      job="astralog-brokers",
      api=~"produce|fetch"
    }[5m])
  )
)
```

Do not average precomputed broker percentiles. Histogram buckets must be aggregated first and `histogram_quantile` applied afterward.

These quantiles are **bucket-interpolated estimates**, not exact nearest-rank
percentiles. Very small request sample counts do not establish useful tail
latency claims. The measured duration covers server-side request handling,
not client-side network round trips or application processing.

Counters reset when a broker process restarts. Prometheus `rate` is intended to
handle counter resets, provided scrape coverage is sufficient. A missing series
or failed scrape is not the same as a measured zero request rate.

### 4. Generate real traffic

Run `cargo run -p astralog-broker --example control_center_demo -- --traffic`
or use `scripts\run-control-center.cmd`. The `--traffic` opt-in produces
actual synthetic records against the disposable real RF3 brokers and fetches
committed data. The metrics reflect real successful wire responses. Historical
benchmark numbers belong only in benchmark reports and are never inserted
as live dashboard values.

### 5. Verify failure visibility

For a controlled local check, stop one broker metrics listener or one broker process. Its Prometheus target should become unreachable. Existing counter rates for that instance will stop receiving new samples and eventually disappear from rate expressions. This is scrape visibility only. The MVP dashboard intentionally does not label the broker as operationally healthy or unhealthy based only on Prometheus scrape status.

## Dashboard panels

The provisioned `AstraLog Engineering MVP` dashboard is organized into four
collapsible rows, rather than putting every broker and cluster aggregate into
the same graphs:

1. **Cluster overview:** four compact values for scrape endpoints reachable
   (0 through 3; scrape reachability is not broker service health),
   acknowledged Produce records/s, completed Produce plus Fetch requests/s,
   and errors/s. Four small charts show acknowledged records/s and value
   bytes/s by broker, plus combined request rate and error rate by API.
2. **Broker 1 (`:9201`):** six focused charts: acknowledged Produce records/s,
   acknowledged value bytes/s, Produce versus Fetch requests/s, Produce versus
   Fetch errors/s, Produce latency p50/p95/p99, and Fetch latency p50/p95/p99.
3. **Broker 2 (`:9202`):** the same six charts, scoped to Broker 2 only.
4. **Broker 3 (`:9203`):** the same six charts, scoped to Broker 3 only.

Click a Grafana row title to collapse or expand its section. Single-series
charts hide their legends; other charts use concise labels such as `Broker 1`,
`produce`, `fetch`, `p50`, `p95`, and `p99` instead of full hostname text.

The dashboard explicitly selects the configured `host.docker.internal:9201`
through `:9203` scrape instances. This avoids briefly displaying legacy
`:9601` through `:9603` series still retained in Prometheus from an earlier
target configuration. Change the dashboard selectors if you intentionally
move the scrape endpoints to different ports.

All counter panels use `rate` over one minute. Per-broker latency panels use
five-minute histogram bucket rates and retain `le` until applying
`histogram_quantile`. Their p50/p95/p99 values are bucket-based estimates,
not exact observed percentiles, and low sample counts cannot justify strong
tail-latency claims. Missing scrape data is not equivalent to zero traffic.

The file is provisioned from
`observability/grafana/dashboards/astralog-mvp.json` and is automatically
re-read by Grafana. With the stack running, refresh the browser after a few
seconds to see the updated layout; no new database or credentials are needed.

## MVP boundaries

This stack is an interview ready local monitoring MVP, not full P11 observability qualification. It does not yet establish alerting, long term retention, remote authentication, TLS, distributed tracing, log aggregation, restart continuity guarantees, SLOs, or production incident response procedures.

The opt-in metrics endpoint is loopback-only and disabled for ordinary broker
startup. The disposable demonstration enables it on 9201 through 9203. The
test-only RF3 traffic mode is an actual workload, not a benchmark. The Rust
test `crates/astralog-broker/tests/monitoring_integration.rs` verifies that a
real Produce and Fetch increment counters and histograms through HTTP `/metrics`.

The current Prometheus names are a contract between the broker instrumentation and this stack. If the broker implementation changes a metric name or label set, update the dashboard and this document in the same change so Prometheus does not silently return empty series.
