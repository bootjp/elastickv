# Getting started with Elastickv

Start with one persistent node on your own machine, write a key through Redis, and check that it survives a restart.
This guide uses the normal server entrypoint at the repository root.
For a replicated cluster on separate machines, continue with [Kubernetes](kubernetes.md) or the [multi-VM Docker runbook](docker_multinode_manual_run.md).

## Prerequisites

Choose one way to run the server:

| Method | Requirements | Use it for |
| --- | --- | --- |
| Build from source | Git and Go 1.26 or later; `go.mod` selects the build toolchain | Trying this checkout on Linux or macOS |
| Docker | Docker Engine or Docker Desktop with Linux container support | Running an image without installing Go |

Elastickv supports production use. These steps start with a local single-node instance; for a production deployment with node-failure tolerance, continue with the [deployment section](#continue-to-deployment).

Both methods use `curl` and `redis-cli` on the host for verification.
Check them with `curl --version` and `redis-cli --version`.
The first Go build downloads dependencies and may download the toolchain selected in `go.mod`.
For an air-gapped deployment, prepare a binary or container image for the target OS and architecture, plus the client tools, before moving them into the isolated network. The clone, dependency downloads, and public-registry image pull shown here require network access during preparation; a prebuilt server can run with local data and static credentials.
The published image workflow currently builds `linux/amd64`; on an ARM machine, use the source build or configure Docker's amd64 emulation.

The examples bind client ports to loopback because the Redis, DynamoDB, and default gRPC endpoints are unauthenticated.
Keep cluster and management ports on a trusted private network when moving beyond local evaluation.
A single node persists data but cannot tolerate loss of that node or its disk.

## Build and start a node

Run from a new terminal:

```bash
git clone --recurse-submodules https://github.com/bootjp/elastickv.git
cd elastickv
go build -o elastickv .
./elastickv \
  --address 127.0.0.1:50051 \
  --redisAddress 127.0.0.1:6379 \
  --dynamoAddress 127.0.0.1:8000 \
  --metricsAddress 127.0.0.1:9090 \
  --healthAddress 127.0.0.1:8081 \
  --pprofAddress= \
  --raftId n1 \
  --raftDataDir .cache/quickstart-data \
  --raftBootstrap
```

If you already cloned the repository, start at `go build` in its root directory.
The server stays in the foreground and prints logs; leave this terminal open.
`--raftBootstrap` creates the initial membership of a fresh cluster.
Use it only for first startup, then remove it from the restart command.
The explicit data directory keeps the Raft log and Pebble MVCC data across restarts.
`.cache/` is ignored by Git; treat this directory as database data and preserve it when cleaning build caches.

These are the listeners enabled by this example:

| Address | Purpose |
| --- | --- |
| `127.0.0.1:6379` | Redis-compatible client API |
| `127.0.0.1:50051` | gRPC API and Raft communication |
| `127.0.0.1:8000` | DynamoDB-compatible HTTP API |
| `127.0.0.1:9090/metrics` | Prometheus metrics |
| `127.0.0.1:8081/readyz` | Local runtime readiness |

S3, SQS, the admin dashboard, and FUSE are opt-in and remain disabled here.
`--pprofAddress=` disables the default debug listener.

### Docker alternative

Use this instead of the source startup above; stop any existing server using the same host ports first.
The image uses `/app` as its command, so supply `/app` explicitly before the flags.

```bash
docker volume create elastickv-quickstart-data
docker run --name elastickv-quickstart \
  -p 127.0.0.1:6379:6379 \
  -p 127.0.0.1:8081:8081 \
  -v elastickv-quickstart-data:/var/lib/elastickv \
  ghcr.io/bootjp/elastickv:latest /app \
  --address 0.0.0.0:50051 \
  --redisAddress 0.0.0.0:6379 \
  --dynamoAddress 127.0.0.1:8000 \
  --metricsAddress 127.0.0.1:9090 \
  --healthAddress 0.0.0.0:8081 \
  --pprofAddress= \
  --raftId n1 \
  --raftDataDir /var/lib/elastickv \
  --raftBootstrap
```

Only Redis and health are published to the host; gRPC, DynamoDB, and metrics stay inside the container.
The wildcard gRPC bind is for this isolated single-node example only; multi-node Raft addresses must be reachable, concrete node addresses.
`latest` tracks main and is convenient for evaluation. For repeatable deployments, select and validate an immutable image digest.
The image must include `--healthAddress` support; build this checkout with `docker build -t elastickv-local .` and substitute `elastickv-local` if the image you selected predates that flag.

## Verify readiness and write a key

In a second terminal:

```bash
curl --fail http://127.0.0.1:8081/readyz
```

HTTP 200 means the local runtime is ready.
HTTP 503 can occur while the node recovers its log or elects a leader; retry after a few seconds.
`/livez` checks process responsiveness, while `/readyz` checks local Raft recovery, membership, leader contact, and apply progress.
Readiness alone does not prove cluster quorum or API correctness, so also perform the following read/write check:

```bash
redis-cli -h 127.0.0.1 -p 6379 SET greeting hello
# OK
redis-cli -h 127.0.0.1 -p 6379 --raw GET greeting
# hello
```

For a source-run node, metrics are available with:

```bash
curl --fail --silent http://127.0.0.1:9090/metrics
```

See the [monitoring guide](../monitoring/README.md) for Prometheus and Grafana.

## Stop, restart, and check persistence

For the source-run server, press Ctrl+C in its terminal and wait for shutdown to complete.
Restart from the same repository directory with the same flags, omitting `--raftBootstrap`:

```bash
./elastickv \
  --address 127.0.0.1:50051 \
  --redisAddress 127.0.0.1:6379 \
  --dynamoAddress 127.0.0.1:8000 \
  --metricsAddress 127.0.0.1:9090 \
  --healthAddress 127.0.0.1:8081 \
  --pprofAddress= \
  --raftId n1 \
  --raftDataDir .cache/quickstart-data
```

Wait for `/readyz` to return HTTP 200 again, then run:

```bash
redis-cli -h 127.0.0.1 -p 6379 --raw GET greeting
# hello
```

For Docker, run `docker stop --time 30 elastickv-quickstart` in another terminal.
After it stops, remove the container with `docker rm elastickv-quickstart`, then repeat the Docker startup command without `--raftBootstrap`.
Keep the named volume `elastickv-quickstart-data`; removing the container does not remove that volume.

Keep the node ID and data directory together on restart.
To start an independent experiment, select a new empty data directory (or a new Docker volume) and bootstrap that instance.
Do not reuse the bootstrap procedure to replace a lost member of an existing cluster; use the [learner membership workflow](raft_learner_operations.md).

## Enable S3 and SQS

This section extends the **source-run** node above and requires the AWS CLI.
Stop the server first and create a local development credentials file:

```bash
mkdir -p .cache
cat > .cache/quickstart-credentials.json <<'JSON'
{"credentials":[{"access_key_id":"local-access","secret_access_key":"local-secret-for-testing"}]}
JSON
chmod 600 .cache/quickstart-credentials.json
```

These are example credentials for loopback evaluation.
Restart the existing node, adding S3 and SQS listeners and pointing both to the file:

```bash
./elastickv \
  --address 127.0.0.1:50051 \
  --redisAddress 127.0.0.1:6379 \
  --dynamoAddress 127.0.0.1:8000 \
  --metricsAddress 127.0.0.1:9090 \
  --healthAddress 127.0.0.1:8081 \
  --pprofAddress= \
  --raftId n1 \
  --raftDataDir .cache/quickstart-data \
  --s3Address 127.0.0.1:9000 \
  --s3Region us-east-1 \
  --s3CredentialsFile .cache/quickstart-credentials.json \
  --sqsAddress 127.0.0.1:9324 \
  --sqsRegion us-east-1 \
  --sqsCredentialsFile .cache/quickstart-credentials.json
```

Wait for `/readyz` to return HTTP 200.
In the client terminal, set credentials for this shell, then create a bucket and queue:

```bash
export AWS_ACCESS_KEY_ID=local-access
export AWS_SECRET_ACCESS_KEY=local-secret-for-testing
export AWS_DEFAULT_REGION=us-east-1
unset AWS_SESSION_TOKEN AWS_PROFILE

printf 'hello\n' > /tmp/elastickv-hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket quickstart
aws --endpoint-url http://127.0.0.1:9000 s3api put-object \
  --bucket quickstart --key hello.txt --body /tmp/elastickv-hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3api get-object \
  --bucket quickstart --key hello.txt /tmp/elastickv-downloaded.txt
cat /tmp/elastickv-downloaded.txt
# hello

aws --endpoint-url http://127.0.0.1:9324 sqs create-queue --queue-name jobs
aws --endpoint-url http://127.0.0.1:9324 sqs list-queues
```

The object upload/download commands return JSON metadata; `cat` prints the uploaded content. A successful bucket creation may produce no output.
The SQS responses include the new queue URL.
These commands do not edit your saved AWS profiles.
S3 uses path-style requests; both listeners use HTTP even when SigV4 authentication is configured.
For SQS behind a TLS proxy, configure `--sqsPublicEndpoint` to the public HTTP(S) origin so returned queue URLs use that origin.
See the [S3 examples](../README.md#working-with-s3-compatible-storage) and [SQS design](design/2026_04_24_implemented_sqs_compatible_adapter.md) for supported operations.

## Try the separate three-node demo

The demo starts three Raft nodes inside one process and uses temporary storage.
Stop the single-node server first because the demo also uses gRPC port 50051.
From the repository root:

```bash
go run ./cmd/server
```

In another terminal:

```bash
redis-cli -h 127.0.0.1 -p 63791 SET demo-key hello
redis-cli -h 127.0.0.1 -p 63792 --raw GET demo-key
redis-cli -h 127.0.0.1 -p 63793 --raw GET demo-key
```

Expect `OK` followed by `hello` from both reads.
The Redis endpoints are `63791`, `63792`, and `63793`; the normal single-node example uses `6379`.
You can also run the bundled gRPC example with `go run ./cmd/client` against this demo.
Stop the demo with Ctrl+C when finished; its data is not preserved for the next run.
The demo's metrics listeners bind on all interfaces on ports 9091–9093 and default to bearer token `demo-metrics-token`; use it only on a trusted development machine.
Three nodes in one process do not provide independent failure domains or a persistence check.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `address already in use` | Stop the other instance or choose unused ports. Check all configured listeners, including DynamoDB, metrics, and health. |
| `Connection refused` | Keep the server terminal open and inspect startup errors. For Docker, check `docker logs elastickv-quickstart` and the published ports. |
| `/readyz` returns 503 | Allow initial election/recovery to finish. If it persists, inspect logs and confirm the original data directory and node ID are in use. Multi-node clusters also need enough reachable voters. |
| Fresh directory fails to start | Bootstrap a new cluster once with `--raftBootstrap`. Restart existing data without it. |
| Key missing after restart | Check the absolute data path and current working directory, or the Docker volume name. The three-node demo uses temporary storage. |
| Unknown flag in a container | The selected image may predate the documentation. Build this checkout or choose a compatible image digest. |
| Admin page says the bundle is missing | Build the SPA with `cd web/admin && npm ci && npm run build`, then rebuild the Go binary. The root Dockerfile builds the SPA automatically. |

## Continue to deployment

Use three or five voting replicas across independent hosts for a deployment that must survive node failures.
Three voters require two for quorum and tolerate one voter failure; five require three and tolerate two.
Follow the deployment runbooks for membership, private addresses, storage, and verification rather than copying the single-node flags across hosts.

- [Architecture overview](architecture_overview.md): how adapters, routing, Raft, and storage fit together.
- [Kubernetes](kubernetes.md) or [Docker on multiple VMs](docker_multinode_manual_run.md): persistent replicated deployments.
- [Admin configuration](admin.md) and [deployment](admin_deployment.md): authentication and dashboard setup.
- [Backup and restore](operations/backup_restore.md): protect and recover stored data.
- [FUSE filesystem](filesystem.md): prerequisites, mounting, and supported filesystem behavior.
