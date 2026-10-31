### Prometheus zookeeper exporter

Exports `mntr` zookeeper's stats in prometheus format.
`zk_followers`, `zk_synced_followers` and `zk_pending_syncs` metrics are available only on cluster leader.

#### Build

`./build.sh` script builds `dabealu/zookeeper-exporter:latest` docker image.
To build image with different name, pass it to `build.sh` as a first arg.

#### Usage

**Note:** starting from zookeeper v3.4.10 it's required to have `mntr` command whitelisted (details: [4lw.commands.whitelist](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html)).

**Warning:** flag to specify target zk hosts is changed since `v0.1.10`, see below

```
Usage of zookeeper-exporter:
  -listen string
        address to listen on (default "0.0.0.0:9141")
  -location string
        metrics location (default "/metrics")
  -timeout int
        timeout for connection to zk servers, in seconds (default 30)
  -zk-hosts string
        comma separated list of zk servers, e.g. '10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181'
  -zk-tls-auth bool
        zk tls client authentication (default false)
  -zk-tls-auth-cert string
        tls certiticate for zk tls client authentication (required if -zk-tls-auth is true)
  -zk-tls-auth-key string
        tls key for zk tls client authentication (required if -zk-tls-auth is true)
  -client-connections
        collect per-IP connections and limit using cons/conf (default false; cached for 60 seconds)
```

#### Optional per-client connection collection

The opt-in `-client-connections` flag adds read-only `conf` and `cons` queries
per target. Both commands must be present in ZooKeeper's
`4lw.commands.whitelist`. It does not change ZooKeeper configuration.
Without the flag, the exporter issues only its existing `mntr` and `ruok`
commands and retains its existing metrics.

The additional metrics are:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `zk_client_connections` | `zk_host`, `client_ip` | Connections from one source IP to this server |
| `zk_max_client_connections_per_ip` | `zk_host` | Effective `maxClientCnxns` from `conf`; 0 means unlimited |
| `zk_client_connections_collection_success` | `zk_host` | 1 for a successful collection, 0 for failure |
| `zk_client_connections_collection_duration_seconds` | `zk_host` | Duration of the most recent collection |
| `zk_client_connections_collection_timestamp_seconds` | `zk_host` | Unix timestamp of the most recent completed attempt |

These are gauges. Counts include connections without established sessions and
aggregate IPv4-mapped IPv6 addresses with their IPv4 equivalent. Session IDs
and ephemeral ports are not labels. Source IPs may represent multiple clients
behind NAT; the metric measures the source address seen by ZooKeeper.

Results, including failures, are cached for 60 seconds within each exporter
process. Concurrent scrapes share the cache and cannot trigger overlapping
additional collections. Each refresh makes two additional TCP connections
per target. Separate exporter processes have separate caches.
Collection is on demand: the first scrape after cache expiry performs the
queries. Set the Prometheus scrape timeout above the expected query duration;
each new command uses the existing `-timeout` for connection establishment
and another bounded read/write deadline. A refresh can take up to approximately
four times `-timeout` if both connections and responses are slow, plus the
existing `mntr`/`ruok` collection. Those existing commands are unchanged.
Responses are limited to 16 MiB per command.

The cache retains only the latest collection per target. Disconnected IPs
disappear after the next successful refresh. Failed, malformed, truncated,
oversized, empty, disallowed, or timed-out command responses produce a logged
error and `collection_success=0`; counts and the limit are omitted rather
than publishing stale or success-shaped fallback data. A blank `cons` list
is valid and exports no client counts. Missing series must not be interpreted
as zero connections without checking collection success.

Example utilization expression (before scrape labels such as `instance` are
added; scope/deduplicate those labels to the intended environment and target):

```promql
100 * zk_client_connections
  / on(zk_host) group_left
    (zk_max_client_connections_per_ip > 0)
```

Only alert on utilization while the corresponding
`zk_client_connections_collection_success` is 1. Alert separately on collection
failure or missing exporter data. A limit of 0 is unlimited and is intentionally
excluded above. Do not divide server-wide `zk_num_alive_connections` by this
per-IP limit. Do not sum duplicate scrape copies.

#### Staging review and rollout

This implementation is based on upstream v0.1.13. It has not been published or
deployed. Do not point deployment automation at an unpublished release.

1. Review the code, tests, and source-IP label/cardinality policy. Run:
   ```sh
   GOWORK=off go test -race -count=1 ./...
   GOWORK=off go vet ./...
   GOWORK=off go test -run '^$' -bench BenchmarkParseClientConnections -benchmem ./...
   ```
2. Select an approved maintained fork and immutable artifact location. Build a
   Linux binary from the reviewed revision for the target node's architecture:
   ```sh
   GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o zookeeper-exporter .
   ```
   Publish through your approved release process; do not overwrite an upstream
   release.
3. Before any deployment, verify a single staging instance's identity,
   architecture, current exporter version, command whitelist, scrape timeout,
   and baseline CPU/RSS, connection totals, and ZooKeeper latency.
4. After deployment approval, install the reviewed binary on only the verified
   staging host and enable `-client-connections` in its exporter command.
   The upstream v0.1.13 binary does not support this flag. Restarting the
   exporter does not require restarting ZooKeeper.
5. Compare the new per-IP counts with local socket counts, allowing for sampling
   time and collector connections. Verify the limit against `conf`, check
   collection success/duration, and confirm the metrics reach the intended
   Prometheus datasource. Exercise concurrent scrapes, disconnects, and failures
   in a controlled environment, not by disrupting a shared staging ensemble.
6. Measure CPU, RSS, response sizes, collection latency, and ZooKeeper request
   latency with the flag disabled and enabled over the same workload. Local
   parser benchmarks do not measure ZooKeeper-side `cons` overhead.
   Stop expansion if collection fails, scrapes exceed their timeout, or agreed
   resource/latency budgets regress. Set those budgets from staging baselines
   before rollout; no production overhead percentage has been established.
7. Roll back by restoring the prior artifact and removing `-client-connections`,
   then restarting only the exporter.
   Verify legacy metrics still scrape. Add alert rules only after label
   matching, deduplication, and collection health checks have been validated.

An example `docker-compose.yml` can be used for management of clustered zookeeper + exporters:

```
# start zk cluster and exporters
docker-compose up -d

# get metrics of first exporter (second and third exporters are on 9142 and 9143 ports)
curl -s localhost:9141/metrics

# at 9184 port there's exporter which handles multiple zk hosts
curl -s localhost:9144/metrics

# shutdown containers
docker-compose down -v
```

#### Dashboard

Example grafana dashboard: https://grafana.com/grafana/dashboards/11442
