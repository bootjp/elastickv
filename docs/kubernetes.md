# Deploying and operating Elastickv on Kubernetes

`deploy/kubernetes/` provides generic Kustomize manifests for a new, three-voter cluster with a single Raft group.
They run `/app` from the image built by the repository's root `Dockerfile`, without application changes or a shell inside the container.
**Do not apply these bootstrap manifests to an existing cluster's data directories.**
Plan any existing-cluster migration separately, including backups, restore validation, membership changes, credentials, and client cutover.
Keep environment inventories and migration execution records outside this repository.

## Architecture

| Resource | Purpose |
| --- | --- |
| StatefulSet `elastickv` | Three voters with stable IDs `elastickv-0` through `elastickv-2` |
| Headless Service `elastickv-peer` | Per-Pod DNS for Raft communication |
| ClusterIP Service `elastickv` | gRPC on 50051, Redis on 6379, and DynamoDB on 8000 |
| PVC `data-elastickv-N` | An independent 100 GiB persistent volume for each Pod |
| PodDisruptionBudget | Keep two available Pods during voluntary eviction |
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
Pin the image to a tested commit tag or digest.
The published image workflow currently builds for `linux/amd64`, and the manifests select amd64 nodes.

```sh
kubectl config current-context
kubectl get nodes
kubectl get storageclass elastickv-local
kubectl get pv

# latest is an example. Replace it with a tested digest before deployment.
# In statefulset.yaml, set image to:
# ghcr.io/bootjp/elastickv@sha256:<tested-digest>

kubectl kustomize deploy/kubernetes > /tmp/elastickv-rendered.yaml
kubectl apply --dry-run=server -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply --dry-run=server -k deploy/kubernetes
kubectl apply -k deploy/kubernetes
kubectl -n elastickv get pods,pvc -w
```

If Pods remain Pending, resolve missing StorageClasses, insufficient PVs, or a lack of three eligible nodes.
Do not bypass these requirements by removing anti-affinity and placing all voters on one node.

Label each permitted client namespace.
The label grants API access to every Pod in that namespace, so apply it only to namespaces under your control.
The CNI must enforce NetworkPolicy.
Review network isolation and client authentication requirements before exposing Redis or DynamoDB.

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

Build [raftadmin](raft_learner_operations.md) from the matching source revision.
Port-forward each Pod's port 50051 in turn and inspect `status` and `configuration`.
Stop each port-forward before connecting to the next Pod.

```sh
go build -o /tmp/elastickv-raftadmin ./cmd/raftadmin
kubectl -n elastickv port-forward pod/elastickv-0 50051:50051
# In another terminal:
/tmp/elastickv-raftadmin 127.0.0.1:50051 status
/tmp/elastickv-raftadmin 127.0.0.1:50051 configuration
```

Verify that all Pods agree on a leader, all three voters are registered, `pending_conf_change` is false, and `fsm_pending` is zero.
With writes paused, verify that each Pod's applied index catches up to the leader's commit index.
Use a temporary key to check Redis SET/GET/DEL and test reads and writes through every required API.
Finally, recreate one Pod through normal deletion and verify that its ID, PVC, and data are retained and that it catches up.

## Updates and failure recovery

The StatefulSet uses the `OnDelete` update strategy.
Changing its Pod template does not automatically restart existing Pods.
Before an update, take a backup and inspect every voter's state, then delete one Pod at a time without forcing deletion.
Verify restart, voter registration, replication catch-up, and API reads and writes before proceeding to the next Pod.
The PDB does not prevent direct StatefulSet deletion or `kubectl delete pod`, so these checks remain necessary.

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
The bootstrap member list is fixed at three members; scaling requires the [learner workflow](raft_learner_operations.md) and configuration changes.
Do not recreate a member that has lost its PVC using bootstrap flags alone.

## Add S3, SQS, the admin dashboard, and monitoring

The base manifests enable gRPC, Redis, and DynamoDB.
S3, SQS, the admin dashboard, and FUSE remain disabled.
For an existing deployment, carry every required API and setting into an environment-specific overlay.

- For S3, add `--s3Address=0.0.0.0:9000`, `--s3CredentialsFile`, and `--raftS3Map`, and expose port 9000 in the container, Service, and NetworkPolicy.
- For SQS, add `--sqsAddress=0.0.0.0:9324`, `--sqsCredentialsFile`, and `--raftSqsMap`, and expose port 9324 in those resources.
- Match each map's Raft addresses to the bootstrap member list and use individual Pod DNS names as destinations.
- Mount credentials as files from a Secret. Preserve the existing signing regions, path-style settings, and keys.
- Configure the admin dashboard according to [admin.md](admin.md), including signing keys, access keys, and TLS or an authenticated reverse proxy.
- Metrics bind to loopback by default. External scraping requires `--metricsAddress`, `--metricsToken`, a Secret, and appropriate NetworkPolicy rules.
- If gRPC TLS or authorization is enabled, apply matching settings to peers and operational CLIs.
- FUSE requires additional host privileges; review its [dedicated guide](filesystem.md) and placement requirements separately.

Manage external Services, Ingress, fixed IPs, and DNS changes in the environment-specific overlay.
For APIs that return redirects or endpoint addresses, verify that clients can reach the returned addresses.

## References

- [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Kubernetes Local volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local)
- [Live Logical Backup](operations/backup_restore.md)
- [Snapshot Restore](operations/snapshot_restore.md)
