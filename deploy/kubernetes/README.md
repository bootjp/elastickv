# Kubernetes manifests

See [the deployment guide](../../docs/kubernetes.md) for storage setup, bootstrap, verification, and maintenance.
The base restarts an existing three-voter cluster from persistent state and refuses to bootstrap an empty volume.
Use `bootstrap/` only for initial creation, then return every Pod to the base before admitting application writes.
Prepare `elastickv-local` storage before applying the manifests.
Client access is limited to Redis and DynamoDB; the shared Raft/gRPC control port is reserved for peers and trusted operators.
