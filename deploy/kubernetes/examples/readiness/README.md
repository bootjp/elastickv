# Stateful runtime readiness

Use an image containing the runtime readiness listener. Replace the placeholder
image in `kustomization.yaml` with the published digest before applying this
example. The base remains pinned to an older image and does not enable this flag.

`--healthAddress=0.0.0.0:8081` enables a separate private HTTP listener:

- `/livez`: the process can serve HTTP; it does not wait for snapshot recovery.
- `/readyz` and `/healthz`: every configured local Raft group has completed startup
  restore/replay, is running, is not installing a snapshot, includes this node as
  a voter, and knows a current voter leader. Followers also need recent leader
  contact. The node must reach its initial committed watermark and snapshot index.
  Later committed backlogs are tolerated while application progresses, but an
  observed stall lasting 30 seconds makes the node not ready. Idle nodes remain
  ready; index regression fails closed for the lifetime of the probe.

Readiness returns 200 or 503 with stable JSON reason codes. GET and HEAD are
supported. No peer addresses, store paths, keys, or internal errors are returned.
These checks perform no application writes or disk scans. Existing protocol health
paths keep their previous semantics; this example probes the dedicated port.

The health port is not added to public Services. Kubelet probes reach the Pod
locally; if a trusted operator must poll the port from another Pod, add a narrowly
scoped NetworkPolicy grant for that operator. Do not add broad client ingress.

Liveness uses `/livez`, so a slow snapshot restore is not repeatedly restarted.
Do not substitute readiness for liveness. Release acceptance additionally needs
cluster-wide membership/quorum checks and signed API canaries. Readiness does not
prove end-to-end protocol compatibility, synchronous durability, or every byte of
an on-disk snapshot. Restore success is reported from the actual engine path.

For an existing installation, first roll out the new image with the old probes,
verify its private listener, and only then switch the readiness configuration.
An older image without this endpoint must also recover its old probe settings on
rollback; image-only rollback is safe only when those settings are compatible.
