# Kubernetes manifests

See [the deployment guide](../../docs/kubernetes.md) for storage setup, bootstrap, verification, and maintenance.
The base restarts an existing three-voter cluster from persistent state and refuses to bootstrap an empty volume.
Use `bootstrap/` only for initial creation, then return every Pod to the base before admitting application writes.
Prepare `elastickv-local` storage before applying the manifests.
Client access includes Redis, DynamoDB, S3, and SQS; the shared Raft/gRPC control port is reserved for peers and trusted operators.

S3 and SQS require the `elastickv-api-credentials` Secret.
Optional examples cover [TLS Ingress](examples/https/) and [a FUSE mount shared within a Pod](examples/fuse/).
The FUSE example requires a dedicated image and privileged mount propagation; neither example is installed by the base.

The opt-in [runtime readiness example](examples/readiness/) separates process
liveness from Raft membership, startup recovery, and log-application progress.
It requires a newer image than the base pin; replace its placeholder digest first.

### Filesystem owner recovery

FUSE mounts always use the operation journal; no legacy-mode flag is available.
`--filesystemStrict=false` is rejected. Give each live mount a unique stable
client ID (the default is its Raft ID). After an unclean Pod stop, confirm the
old process is no longer running before using `--filesystemStrictRecover` for
that owner. Do not enable automatic fencing of a possibly live owner during a
force-deletion or split-brain incident. Keep the existing persistent volume and
keys. See [filesystem cutover and recovery](../../docs/filesystem.md#upgrading-from-legacy-mounts).
