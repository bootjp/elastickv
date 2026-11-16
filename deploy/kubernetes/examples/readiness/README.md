# Stateful runtime readiness

The base remains pinned to an older image without the runtime readiness listener.
The `image-first` overlay pins the image built from main commit
`b9043cbb23860d69525b7db405b711e7756dc483` and enables its private health
listener while retaining the base TCP probes. The parent overlay changes only
the probes after every member has been upgraded. Use a newer published digest
only after its startup, restore, and replication checks pass.
The pinned digest is recorded by the [successful main image build](https://github.com/bootjp/elastickv/actions/runs/43813746736).

`--healthAddress=0.0.0.0:8081` enables a separate private HTTP listener:

- `/livez`: the process can serve HTTP; it does not wait for snapshot recovery.
- `/readyz` and `/healthz`: every configured local Raft group has completed startup
  restore/replay, is running, is not installing a snapshot, includes this node as
  a voter, and knows a current voter leader. Followers also need recent leader
  contact. The node must reach its initial committed watermark and snapshot index.
  Later committed backlogs are tolerated while application progresses, but an
  observed stall lasting 30 seconds makes the node not ready. Idle nodes remain
  ready; index regression fails closed for the lifetime of the probe.

A single background worker samples local state once per second. HTTP requests
read its cached result without waiting for engine locks; a missing result or one
older than three seconds returns not ready. A Raft status publication that has
not advanced for 30 seconds also returns not ready, even if its cached indexes
look idle. Unknown (negative) follower contact is rejected, and contact ages
from the engine observation time.

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

For an existing installation:

1. Render `kubectl kustomize deploy/kubernetes/examples/readiness/image-first`
   and apply `kubectl apply -k deploy/kubernetes/examples/readiness/image-first`.
   The StatefulSet uses `OnDelete`: replace
   one Pod at a time, preserving its PVC. After each replacement, verify its
   `/livez` and `/readyz` on the private port, its applied membership and
   catch-up, and cluster quorum before replacing another member. The TCP
   probes remain in force during this stage.
2. Once every member serves the new listener and the cluster-wide checks and
   signed API canaries pass, render and apply
   `deploy/kubernetes/examples/readiness`. Replace one
   Pod at a time again so the new HTTP probes take effect. Confirm that
   `/livez` remains independent of slow recovery and `/readyz` stays 503 until
   the local groups are ready.
3. To roll back to the old image, first restore the base TCP probe settings
   with a compatible image configuration, then replace Pods one at a time.
   Never leave an old image running behind the HTTP probes. Keep the PVCs.

The health port is not in a public Service or NetworkPolicy ingress grant.
