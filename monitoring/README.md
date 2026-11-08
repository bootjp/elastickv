# Monitoring Elastickv

The dashboards and Prometheus alert rules in this directory are reusable across
standalone, Docker, and Kubernetes deployments. They do not configure production
credentials, notification destinations, or a fixed number of voters.

## Collect metrics

Scrape every server's `/metrics` endpoint, not only the leader. Configure the
server's `--metricsAddress` and `--metricsToken` as described in the root README.
The bundled `prometheus/prometheus.yml` and `docker-compose.yml` target the local
three-node demo; replace its targets and demo token for other installations.
Use a private network or authenticated collector rather than exposing metrics
publicly. A collector sidecar can scrape the loopback endpoint without opening
an additional Service port. Configure its remote write URL separately.

Use a stable `job` and a unique `cluster` label on **every collected series**,
including `up`, when a datasource contains multiple deployments. For example,
add `labels: {cluster: example-cluster}` to a Prometheus `static_configs` entry,
or use target relabeling in Kubernetes. Labels applied only as Prometheus
`external_labels` are not attached to local query results. A single deployment
may omit `cluster`; the dashboards and alert aggregations support that case.
Never put access tokens in dashboard JSON or alert annotations.

## Load dashboards

Import the JSON files in `grafana/dashboards/` into Grafana, or use the file
provider in `grafana/provisioning/dashboards/dashboards.yml`. The Docker Compose
example mounts these files and provisions a datasource named Prometheus. For an
existing installation, retain your own datasource URL and authentication settings.

Each dashboard has Datasource, Job, and Cluster selectors. Choose the local
Prometheus-compatible datasource, then a job and cluster. The initial All selection
also matches metrics without a `cluster` label. Node, group, command, table, and
queue selectors are discovered from metrics rather than a static node inventory.
Selecting All across deployments intentionally combines their series in summary
panels; select one cluster when interpreting cluster-wide gauges or leader counts.

The dashboards cover cluster health, Raft, DynamoDB, Redis (including its Hot Path
row), SQS, filesystem activity, and Pebble internals. Panels for disabled adapters
or unmounted filesystems may be empty. Leader-count alerts deduplicate repeated observations of the same node.
No node-exporter job or host label convention
is required. Monitor host resources with a separate node-exporter dashboard.

## Load alerts

`prometheus/rules/*.yml` are **datasource-managed Prometheus rules**, not
Grafana-managed rule exports. This keeps one set of expressions usable with
Prometheus and compatible rulers such as vmalert. Load them using the bundled
Prometheus `rule_files` setting, a ruler's rule-file option, or a Prometheus
Operator `PrometheusRule` resource (copy the `groups` into `spec.groups` and use
labels selected by your Prometheus installation).

Rules cover failed metrics scrapes, invalid Raft leader counts, sustained apply
lag, filesystem placement/cleanup, and TSO allocation and rollout invariants.
They preserve `job` and `cluster` labels so a healthy cluster cannot mask another
cluster's failures. No quorum rule assumes a fixed voter count. Raft rules require
all members to be scraped; observations are not a proof of quorum availability.
Filesystem replicated gauges use `max`, while node-local gauges use `sum`.

Change the `job="elastickv"` matcher in `ElastickvMetricsTargetDown` if your scrape
job has another name. Other rules select Elastickv metric names directly. Tune
thresholds and durations for the workload. Protocol-specific metrics absent from
a deployment produce no alerts. `up == 0` detects failed configured targets, not
an entirely missing collector, deleted discovery target, or remote-write outage;
monitor collector/ruler health and expected inventory separately.

Configure Prometheus or the ruler to send alerts to your own Alertmanager, and
configure notification routing there. The Compose example evaluates rules but
does not configure a notification receiver. In Grafana, enable viewing
Prometheus datasource-managed rules in that datasource's settings; supported
rulers can also be configured for rule management. Dashboard variables are not
available inside alert expressions. Importing dashboard JSON alone does not
install alerts, and Grafana notification policies do not automatically route
alerts evaluated by Prometheus.

## Validate changes

From this directory, using a compatible `promtool`:

```sh
promtool check rules prometheus/rules/*.yml
promtool test rules prometheus/tests/alerts.yml
```

From the repository root, run `go test ./monitoring`. The rule fixtures exercise
independent clusters and deployments without a cluster label.

References: [Grafana datasource-managed alert visibility](https://grafana.com/docs/grafana/latest/datasources/prometheus/alerting/)
and [Prometheus rule testing](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).
