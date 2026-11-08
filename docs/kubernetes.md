# Deploying and operating Elastickv on Kubernetes

`deploy/kubernetes/` provides generic Kustomize manifests for a new, three-voter cluster with a single Raft group.
The steady-state base restarts members from persisted Raft state and refuses to bootstrap empty data directories.
The `bootstrap/` overlay is only for initial cluster creation.
Both run `/app` from the image built by the repository's root `Dockerfile`, without application changes or a shell inside the server container.
**Do not apply the bootstrap overlay to an existing cluster or a replacement PVC.**
Plan any existing-cluster migration separately, including backups, restore validation, membership changes, credentials, and client cutover.
Keep environment inventories and migration execution records outside this repository.

## Architecture

| Resource | Purpose |
| --- | --- |
| StatefulSet `elastickv` | Three voters with stable IDs `elastickv-0` through `elastickv-2` |
| Headless Service `elastickv-peer` | Per-Pod DNS for Raft communication |
| ClusterIP Service `elastickv` | Client Redis on 6379, DynamoDB on 8000, S3 on 9000, and SQS on 9324 |
| ClusterIP Service `elastickv-control` | Shared Raft/gRPC port 50051 for trusted operators only |
| PVC `data-elastickv-N` | An independent 100 GiB persistent volume for each Pod |
| PodDisruptionBudget | Block all voluntary evictions until maintenance is explicitly coordinated |
| NetworkPolicy | Allow traffic from cluster peers and explicitly permitted client namespaces |

Initial startup combines `Parallel` Pod management with `publishNotReadyAddresses: true`.
All voters can start without waiting for another Pod to become Ready, and peer DNS records are published before leader election.
Each Raft address is `<pod-name>.elastickv-peer.<namespace>.svc:50051`.
The Downward API supplies the namespace, so it can be changed through Kustomize.
DNS negative caching can delay initial name resolution.

Pods must run on different Kubernetes nodes.
For virtualized deployments, also place the VMs and their disks on different physical hosts.
Three voters tolerate one voter failure.
Putting every PVC on one NAS makes that NAS a shared storage failure domain.

## Prepare storage

The manifests reference StorageClass `elastickv-local`.
Prepare this StorageClass and its PVs before applying the manifests; the base does not create them.
To use an existing CSI storage class, change `volumeClaimTemplates[].spec.storageClassName` and validate fsync, locking, and failure behavior.

A local PV example is available in `deploy/kubernetes/examples/local-storage.yaml`.
After formatting and persistently mounting a dedicated disk, replace the example node name and mount path.
Create three uniquely named PVs, one on each of three different nodes.
Do not point `local.path` at an empty directory on the OS filesystem as a substitute for the dedicated disk.
Use `findmnt` on each node and verify that the disk remains mounted after a reboot.
Make the directory readable and writable by UID/GID `65532:65532`.

A local PV's `capacity` declares its capacity; it does not impose a filesystem quota on the directory.
Monitor free space and keep the data volume separate from the OS filesystem.
Local PVs remain bound to their original nodes and do not automatically move their disks to another node after a failure.
Recovery requires restoring the original disk or replacing the failed member through the learner workflow.

## Start a new cluster

Run the following commands from the repository root.
For a deployment, maintain a copy of the manifests or an environment-specific Kustomize overlay.
The base image is pinned by digest to the build of commit `2f9a88153672d18dcc9d71abfb4b6bdd3ea9983b`.
This historical image predates `--sqsPublicEndpoint` and the SignedHeaders compatibility fixes required by AWS CLI S3/SQS clients.
For authenticated S3/SQS use, build and publish this source revision and replace the base image with the resulting immutable digest before applying the manifests.
The fixed image will be published through the normal release workflow after merge; do not assume an unmerged commit's image exists in the registry.
For another release, update the digest in `deploy/kubernetes/base/statefulset.yaml` and validate that release before deployment.
The published image workflow currently builds for `linux/amd64`, and the manifests select amd64 nodes.

```sh
kubectl config current-context
kubectl get nodes
kubectl get storageclass elastickv-local
kubectl get pv

kubectl kustomize deploy/kubernetes/bootstrap > /tmp/elastickv-rendered.yaml
kubectl apply --dry-run=server -f deploy/kubernetes/base/namespace.yaml
kubectl apply -f deploy/kubernetes/base/namespace.yaml
# Supply a private credentials file with real keys; never commit it.
kubectl -n elastickv create secret generic elastickv-api-credentials \
  --from-file=credentials.json=/secure/path/credentials.json
kubectl apply --dry-run=server -k deploy/kubernetes/bootstrap
kubectl apply -k deploy/kubernetes/bootstrap
kubectl -n elastickv get pods,pvc -w
```

Do not admit application writes during bootstrap.
After all three members have converged, apply the steady-state base with `kubectl apply -k deploy/kubernetes`.
Because the StatefulSet uses `OnDelete`, this changes only the template: existing Pods still carry bootstrap arguments, including during container restarts.
Recreate each Pod in turn using the maintenance checks below.
Before admitting writes, inspect every Pod's actual `spec.containers[].args` and the StatefulSet template to ensure that neither `--raftBootstrap` nor `--raftBootstrapMembers` remains.
Retire the bootstrap overlay from deployment automation; all later updates use the base.

If Pods remain Pending, resolve missing StorageClasses, insufficient PVs, or a lack of three eligible nodes.
Do not bypass these requirements by removing anti-affinity and placing all voters on one node.

Label each permitted client namespace.
The label grants Redis, DynamoDB, S3, and SQS access to every Pod in that namespace, so apply it only to namespaces under your control.
It does not grant access to port 50051, which also exposes unauthenticated RaftAdmin membership mutators.
The client Service deliberately omits gRPC.
RawKV/TransactionalKV clients must not receive control-port access unless they are trusted operators; supporting untrusted gRPC clients requires a separately authenticated and authorized listener or proxy.
Do not simply add port 50051 to the client NetworkPolicy.
Restrict namespace labeling and Pod creation in the server and operator namespaces through RBAC; creating a peer-labeled Pod in the server namespace would grant peer access.
The CNI must enforce NetworkPolicy.
Review network isolation and client authentication requirements before exposing client APIs.
S3 and SQS require the `elastickv-api-credentials` Secret; missing credentials block startup rather than enable anonymous service.
The file format is `{"credentials":[{"access_key_id":"YOUR_ACCESS_KEY","secret_access_key":"YOUR_SECRET_KEY"}]}`.
The base uses `us-east-1` and path-style S3 requests; configure clients with matching credentials and region.

```sh
kubectl label namespace <client-namespace> elastickv-client=true
# Client endpoint: elastickv.elastickv.svc:6379
kubectl -n elastickv port-forward service/elastickv 6379:6379
# In another terminal: redis-cli -p 6379 PING
```

## Verify startup and replication

Startup and liveness probes check TCP connectivity to the Raft port.
The readiness probe checks TCP connectivity to the Redis port.
These probes check listener availability; they do not establish quorum, replication catch-up, or successful writes.
The existing gRPC health status depends on leadership, so it is unsuitable as a liveness check that would repeatedly restart followers.

Use an in-cluster operator Pod to connect to each member's DNS address.
The server binds gRPC directly to its Pod DNS/IP, while containerd port-forwarding connects to loopback inside the Pod network namespace.
Consequently, `kubectl port-forward` to port 50051 is not a usable path for this configuration.
The optional operator example creates a separate namespace labeled `elastickv-operator=true`; only trusted administrators should be able to create Pods or exec there.
It is not installed by the base.

Build [raftadmin](raft_learner_operations.md) from the source revision matching the deployed image (the pinned default uses commit `2f9a88153672d18dcc9d71abfb4b6bdd3ea9983b`).
Use a separate source checkout if needed; do not change a checkout with uncommitted work.
Build a static Linux/amd64 binary, then copy it through stdin into the writable tools volume:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/elastickv-raftadmin ./cmd/raftadmin
kubectl apply -f deploy/kubernetes/examples/operator.yaml
kubectl -n elastickv-ops wait --for=condition=Ready pod/elastickv-operator --timeout=120s
kubectl -n elastickv-ops exec -i elastickv-operator -- sh -c \
  'cat > /tmp/raftadmin && chmod 0700 /tmp/raftadmin' < /tmp/elastickv-raftadmin

raftadmin() {
  kubectl -n elastickv-ops exec elastickv-operator -- \
    env RAFTADMIN_ALLOW_INSECURE=true /tmp/raftadmin "$@"
}
for ordinal in 0 1 2; do
  endpoint="elastickv-${ordinal}.elastickv-peer.elastickv.svc:50051"
  raftadmin "$endpoint" status
  raftadmin "$endpoint" configuration
done
```

The plaintext override is limited to this trusted operational path; it does not authorize ordinary clients to reach the control port.
Adjust DNS names if the server namespace differs.
Delete the tools Pod after maintenance with `kubectl -n elastickv-ops delete pod elastickv-operator`.
Remove the operator namespace label when the operational access is no longer needed.

Verify that all Pods agree on a leader, all three voters are registered, `pending_conf_change` is false, and `fsm_pending` is zero.
With writes paused, verify that each Pod's applied index catches up to the leader's commit index.
Use a temporary key to check Redis SET/GET/DEL and test reads and writes through every required API.
Finally, recreate one Pod through normal deletion and verify that its ID, PVC, and data are retained and that it catches up.

## Updates and failure recovery

The StatefulSet uses the `OnDelete` update strategy.
Changing its Pod template does not automatically restart existing Pods.
Before an update, take a backup and inspect every voter's state, then delete one Pod at a time without forcing deletion.
Verify restart, voter registration, replication catch-up, and API reads and writes before proceeding to the next Pod.
The PDB uses `maxUnavailable: 0`, so automated eviction and ordinary node drains cannot remove a voter even after a TCP probe becomes Ready.
This deliberately blocks automatic sequential drains because TCP readiness does not prove catch-up.
The PDB does not prevent direct StatefulSet deletion or `kubectl delete pod`.
Restrict those operations to operators, serialize all maintenance, and do not lower the PDB as a shortcut.
For node maintenance, cordon only the target node, stop one member through an operator-controlled deletion, and restore that node and member before touching another.
Local PVs keep the Pod pinned to its node, so it may remain Pending until that node is available again.
After uncordoning the recovered node, verify a common leader, voter membership, no pending configuration change, drained FSM work, catch-up to a recorded leader commit index, and successful writes before authorizing the next deletion or node operation.
A node drain that needs to evict these Pods will remain blocked; arrange a controlled outage or an explicit member replacement instead of using eviction bypass flags.

```sh
kubectl apply -k deploy/kubernetes
kubectl -n elastickv delete pod elastickv-2
# Complete the state and API checks above before continuing with Pods 1 and 0.
```

`terminationGracePeriodSeconds: 90` allows time for leadership transfer, connection draining, and store shutdown.
Measure shutdown on slow disks and increase the grace period if needed.
Before force-deleting a Pod after a node failure, ensure that its old process cannot continue running.

PVCs are retained when the StatefulSet is deleted or scaled down.
Deleting the namespace also deletes its PVCs; do not use namespace deletion as a shutdown procedure.
Retain PVCs, PVs, and data directories until backups and recovery have been verified.
Changing `replicas` alone does not change Raft membership.
The initial bootstrap member list is fixed at three members; scaling requires the [learner workflow](raft_learner_operations.md) and configuration changes.
Do not recreate a member that has lost its PVC using bootstrap flags alone.

## Replace a lost PVC through a learner

The base has no bootstrap or join flags.
A replacement volume has no persisted peers, so normal startup fails rather than creating the original voter configuration again.
Never fix this by applying the bootstrap overlay.
The following example replaces only `elastickv-2` in a single-group cluster; adapt the target and peer list to the live configuration before running it.

1. Freeze all other maintenance. Fence the old process and retain its disk or backup so it cannot return with stale membership.
2. Using the operator tools, find the surviving leader and record its current configuration index. Remove the failed member with `raftadmin <leader-dns:50051> remove_server elastickv-2 <configuration-index>`. Verify the committed configuration contains only the two surviving voters and no pending change.
3. Prepare a fresh dedicated PV/PVC for the target ordinal, retaining the old data separately. Do not delete a healthy member's volume. If the StatefulSet starts the target early, the steady-state base will refuse its empty state.
4. Inspect `deploy/kubernetes/examples/replacement/kustomization.yaml`. Its join-discovery list must contain the current survivors and the target's exact ID/address. Apply it with `kubectl apply -k deploy/kubernetes/examples/replacement`, then normally delete only `elastickv-2` so it picks up the learner flags. Existing Pods are unchanged because updates use `OnDelete`.
5. Call `raftadmin <leader-dns:50051> add_learner elastickv-2 elastickv-2.elastickv-peer.elastickv.svc:50051 <configuration-index>`, using the latest committed configuration index. Confirm that the target appears as a learner.
6. Record a nonzero leader commit index, wait until the learner has applied through it, then call `raftadmin <leader-dns:50051> promote_learner elastickv-2 <configuration-index> <recorded-commit-index>`. Do not disable the catch-up check.
7. Verify three voters, no pending configuration change, and successful reads and writes. Apply the base again with `kubectl apply -k deploy/kubernetes`, then normally recreate only the replacement Pod to remove its learner flags. Verify catch-up again and inspect the actual Pod and template arguments before releasing the maintenance lock.

The replacement overlay is a temporary template for one failed member, not a permanent mode for every Pod.
Do not recreate another member while it is active.
A container restart of a surviving Pod uses its existing Pod specification; deleting another Pod would instead pick up the temporary learner template.
See the [learner operations guide](raft_learner_operations.md) for membership commands and recovery constraints.

## S3 and SQS client endpoints

The base starts S3 and SQS with SigV4 credentials mounted from a Secret.
The ClusterIP endpoints are `elastickv.elastickv.svc:9000` and `elastickv.elastickv.svc:9324`.
The peer maps use per-Pod DNS names for internal forwarding, and permitted client namespaces can reach both ports.
The shared Raft/gRPC port remains isolated from ordinary clients.
For an existing deployment, carry the existing API settings and credentials into its environment-specific overlay.

For local checks, use `kubectl -n elastickv port-forward service/elastickv 9000:9000 9324:9324`.
With matching AWS CLI credentials configured outside the repository, test S3 bucket creation, upload, download, and deletion through `http://127.0.0.1:9000`.
For SQS, test queue creation, send, receive, delete-message, and delete-queue through `http://127.0.0.1:9324`.
Verify the same operations after a leader change, including the URLs returned to clients.

### Optional HTTPS exposure outside the cluster

Keep the base Service as ClusterIP.
The example at `deploy/kubernetes/examples/https/` exposes only S3 and SQS through a TLS Ingress, so the same base works for internal, LAN, or internet-facing deployments.
The Ingress Controller, public DNS, certificate, and network boundary are deployment-specific.
The example domains under `example.test` and `example-controller` ingress class are placeholders.

Before applying the example:

1. Build and publish the root Dockerfile from a source revision containing `--sqsPublicEndpoint`. The base's pinned historical image predates that flag and the AWS CLI SignedHeaders compatibility fixes. Replace the example's deliberately unusable image reference with the new image's immutable digest.
2. Replace the two hostnames, ingress class, and `--sqsPublicEndpoint=https://sqs.example.test` with the intended public endpoints. Configure the same SQS public endpoint on every member. Only an HTTP(S) origin is accepted; credentials, path prefixes, query strings, and fragments are rejected.
3. Create the `elastickv-api-tls` Secret in the server namespace from a certificate and private key covering both hosts. Keep the SigV4 credentials Secret configured as above.
4. Label the trusted Ingress Controller namespace with `elastickv-ingress=true`. The example's NetworkPolicy allows that namespace to reach only ports 9000 and 9324. Restrict permission to change this label. Controllers using host networking or different source identities need a policy tailored to the CNI's actual traffic path.
5. Configure the controller to reject or redirect plaintext HTTP and enforce the intended source restrictions. An Ingress `tls` stanza alone does not require HTTPS. Backend HTTP must remain inside the trusted network; use an encrypted upstream configuration if that network is untrusted.
6. Preserve the signed Host, path, and query string; do not rewrite API paths. Configure streaming, upload limits, and timeouts for S3 objects and SQS long polling using the chosen controller's settings.

```sh
kubectl apply --dry-run=server -k deploy/kubernetes/examples/https
kubectl apply -k deploy/kubernetes/examples/https
```

Apply this example to an initialized cluster and use the one-member update workflow.
Its template includes the SQS public endpoint flag; keep using the environment's HTTPS overlay for subsequent updates and member replacements rather than reverting to a base that omits that setting.
Compose the necessary learner flags with the same public settings during replacement.
A brand-new deployment must compose its public settings with the bootstrap overlay first, then remove only the bootstrap flags before admitting writes.

`--sqsPublicEndpoint` controls the URLs returned by CreateQueue, GetQueueUrl, and ListQueues.
It does not enable TLS itself and does not trust `X-Forwarded-*` headers.
Without it, URLs continue to derive their scheme from the request's actual TLS state and their host from the request.
Verify that every returned QueueUrl uses the public HTTPS origin and can be used for subsequent signed requests, including when a follower forwards the request.

## Share the FUSE filesystem within a Pod

The example at `deploy/kubernetes/examples/fuse/` enables the filesystem on the server and shares its mount with a second container in the same Pod.
It does not expose a filesystem network port or an NFS/SMB server.
The file tree remains stored in Elastickv's replicated store.
The example consumer is an idle placeholder; replace it with the application that needs filesystem access.

The standard distroless image lacks `fusermount3`.
Build the example Dockerfile, which adds `fuse3`, `util-linux`, and a named UID/GID 65532 account to a Debian runtime while copying `/app` from a pinned server image:

```sh
docker build --platform linux/amd64 \
  --build-arg ELASTICKV_IMAGE=ghcr.io/bootjp/elastickv@sha256:<server-digest> \
  -f deploy/kubernetes/examples/fuse/Dockerfile \
  -t <registry>/elastickv-fuse:<version> .
docker push <registry>/elastickv-fuse:<version>
```

Replace `example.invalid/elastickv-fuse:build-required` in the overlay with the resulting image digest before applying it.
Choose a server image containing every flag required by the deployment; an HTTPS/FUSE combination needs the new SQS public endpoint flag as well.
The example applies to an initialized cluster and inherits the base's Secret requirements, persistent data, and client APIs.

The node must provide `/dev/fuse`.
The server container is privileged, uses unconfined seccomp/AppArmor, and mounts `/dev/fuse` plus a memory-backed shared directory.
Bidirectional mount propagation exports the mount to that directory; the consumer uses HostToContainer propagation to receive it.
The mountpoint is `/shared/fs` in both containers.
These permissions belong only in an explicitly approved namespace/node configuration; they are not added to the base.
Pod Security admission or local policy may intentionally reject this example until that approval is in place.

Both containers inherit UID/GID 65532 because the filesystem does not enable `allow_other`.
Use the same UID for the consuming application and ensure the persisted filesystem root is owned by that UID.
`--filesystemRootUID` and `--filesystemRootGID` set ownership only at first root creation; they do not repair an existing root.
Readiness checks the FUSE mount and performs directory I/O rather than relying on the Redis listener alone.
The memory-backed shared directory carries the mount; its size limit is not a quota for the replicated filesystem.

```sh
kubectl apply --dry-run=server -k deploy/kubernetes/examples/fuse
kubectl apply -k deploy/kubernetes/examples/fuse
# Recreate one member at a time and verify catch-up using the maintenance workflow.
kubectl -n elastickv exec elastickv-0 -c filesystem-consumer -- \
  sh -ec 'mkdir -p /shared/fs/check; printf "hello\n" > /shared/fs/check/file; cat /shared/fs/check/file'
```

Check that a consumer on another member sees the file through a fresh open, then verify data after a server restart and remove the test file.
Before starting the consuming workload, check the mount's actual I/O; a stale mount can remain in the mount table after a crash.
Applications must stop using the filesystem before the server unmounts it.
Review [filesystem.md](filesystem.md) for supported operations, per-mount locking limits, cache behavior, and crash recovery.
Container startup alone does not validate mount propagation: run a create/read/write/delete and restart test on the intended Linux nodes.
If combining HTTPS and FUSE, compose both sets of settings into one environment overlay; applying either standalone example replaces the StatefulSet template and loses the other's additions.

## Add the admin dashboard and monitoring

The admin dashboard remains disabled in the base.
Configure it according to [admin.md](admin.md), including signing keys, access keys, and TLS or an authenticated reverse proxy.
Metrics bind to loopback by default; external scraping requires `--metricsAddress`, `--metricsToken`, a Secret, and appropriate NetworkPolicy rules.
If gRPC TLS or authorization is enabled, apply matching settings to peers and operational CLIs.

## References

- [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Kubernetes Local volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local)
- [Kubernetes Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/)
- [Kubernetes mount propagation](https://kubernetes.io/docs/concepts/storage/volumes/#mount-propagation)
- [Kubernetes disruption budgets](https://kubernetes.io/docs/tasks/run-application/configure-pdb/)
- [Live Logical Backup](operations/backup_restore.md)
- [Snapshot Restore](operations/snapshot_restore.md)
