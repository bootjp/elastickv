# Portable monitoring assets

## Scope

Make the repository dashboards and operational alerts reusable across deployments.
Dashboard queries select a Prometheus datasource, scrape job, and optional cluster
label; node, group, and protocol dimensions remain discoverable. Defaults include
all matching series, including deployments without a cluster label. Public assets
must contain no deployment addresses, private datasource identifiers, credentials,
or notification destinations.

Alerts are datasource-managed Prometheus rules, usable with Prometheus-compatible
rulers and visible through Grafana's datasource-managed alert view. Aggregations
preserve job and cluster identity so independent deployments cannot mask each
other. Existing filesystem and TSO alerts retain their semantics; add core scrape
and Raft health checks without assuming a fixed voter count. Notification routing
and production scrape credentials remain operator configuration.

## Decisions

- Keep one canonical set of alert expressions rather than duplicate expressions
  in Grafana-managed rules. Document the distinction and loading procedure.
- Preserve existing dashboard UIDs and filesystem gauge aggregation invariants.
- Provisioned demo datasource remains named Prometheus; imports select a local
  datasource through a dashboard variable.
- Do not change the running monitoring installation as part of publishing assets.

## Validation

Parse every dashboard and query, check rules with promtool, exercise alert
isolation across clusters (including an unlabeled deployment), and run monitoring
package tests. Check the patch for environment identifiers before publishing.

## Review

Reviewed against the requested reusable dashboard/alert scope: no application,
storage, or production rollout changes are required. The implementation follows
this proposal; optional protocol alerts activate only when those metrics exist.

## Implementation and review evidence

All seven existing dashboards now select datasource, job, and optional cluster.
Filesystem and TSO rules preserve deployment labels. Core Raft rules detect
unreachable scrape targets, invalid leader counts, and sustained apply lag.
Leader counts deduplicate observations of the same node. The monitoring guide
covers imports, file provisioning, collection labels, ruler setup, and routing.

Five review passes:

1. Data loss: configuration assets only; no application or live deployment writes.
2. Concurrency and distributed failures: rules isolate job/cluster; leader counts
   deduplicate replicas and do not claim to prove quorum availability.
3. Performance: dashboard variable discovery runs on load/time-range refresh;
   alert aggregations retain deployment identity but discard unneeded dimensions.
4. Data consistency: cluster-wide filesystem gauges keep max aggregation;
   node-local activity keeps sum aggregation. No fixed membership assumptions.
5. Test coverage: promtool fixtures cover failure isolation, absent cluster
   labels, duplicate scrapes, TSO mode transitions, and alert hold durations.

Validation: promtool checked all 16 alert rules and all 186 panel expressions.
All seven promtool rule scenarios and `go test ./monitoring` passed before
publication. The full repository commit hook currently fails on a pre-existing
cyclomatic-complexity error in `refreshOpenHandleLease` in the FUSE adapter;
this change does not modify that implementation.
