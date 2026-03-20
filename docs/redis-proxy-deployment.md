# redis-proxy Deployment Guide

redis-proxy is a Redis-protocol reverse proxy that enables gradual migration from Redis to ElasticKV through dual-write, shadow-read comparison, and phased primary cutover.

## Docker Image

Pre-built images are published to GitHub Container Registry on every push to `main`:

```
ghcr.io/bootjp/elastickv/redis-proxy:latest
ghcr.io/bootjp/elastickv/redis-proxy:sha-<commit>
```

The CI workflow (`.github/workflows/redis-proxy-docker.yml`) builds the image automatically when files under `cmd/redis-proxy/`, `proxy/`, or `Dockerfile.redis-proxy` change.

### Building locally

```bash
# Docker
docker build -f Dockerfile.redis-proxy -t redis-proxy .

# Binary
go build -o redis-proxy ./cmd/redis-proxy/
```

## Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-listen` | `:6479` | Proxy listen address |
| `-primary` | `localhost:6379` | Primary (Redis) address |
| `-primary-db` | `0` | Primary Redis DB number |
| `-primary-password` | (empty) | Primary Redis password |
| `-secondary` | `localhost:6380` | Secondary (ElasticKV) address |
| `-secondary-db` | `0` | Secondary Redis DB number |
| `-secondary-password` | (empty) | Secondary Redis password |
| `-mode` | `dual-write` | Proxy mode (see below) |
| `-secondary-timeout` | `5s` | Secondary write timeout |
| `-shadow-timeout` | `3s` | Shadow read timeout |
| `-sentry-dsn` | (empty) | Sentry DSN (empty = disabled) |
| `-sentry-env` | (empty) | Sentry environment name |
| `-sentry-sample` | `1.0` | Sentry sample rate |
| `-metrics` | `:9191` | Prometheus metrics endpoint |

## Proxy Modes

Five modes support a phased migration strategy.

| Mode | Reads from | Writes to | Use case |
|------|-----------|-----------|----------|
| `redis-only` | Redis | Redis only | Transparent proxy. Route traffic through the proxy first |
| `dual-write` | Redis | Redis + ElasticKV | Begin data sync. Populate ElasticKV |
| `dual-write-shadow` | Redis (+ shadow compare from ElasticKV) | Redis + ElasticKV | Verify read consistency between backends |
| `elastickv-primary` | ElasticKV (+ shadow compare from Redis) | ElasticKV + Redis | Promote ElasticKV to primary. Redis as fallback |
| `elastickv-only` | ElasticKV | ElasticKV only | Migration complete. Decommission Redis |

### Recommended Migration Path

```
redis-only -> dual-write -> dual-write-shadow -> elastickv-primary -> elastickv-only
```

Monitor metrics at each stage and roll back to the previous mode if issues arise. Mode changes require a proxy restart.

## Deployment Examples

### Minimal (redis-only)

```bash
docker run --rm \
  ghcr.io/bootjp/elastickv/redis-proxy:latest \
  -listen :6379 \
  -primary redis.internal:6379 \
  -mode redis-only
```

Point your application at the proxy. Behavior is identical to connecting directly to Redis.

### Dual-Write with Shadow Comparison

```bash
docker run --rm \
  -p 6379:6479 \
  -p 9191:9191 \
  ghcr.io/bootjp/elastickv/redis-proxy:latest \
  -listen :6479 \
  -primary redis.internal:6379 \
  -primary-password "${REDIS_PASSWORD}" \
  -secondary elastickv.internal:6380 \
  -mode dual-write-shadow \
  -secondary-timeout 5s \
  -shadow-timeout 3s \
  -sentry-dsn "${SENTRY_DSN}" \
  -sentry-env production \
  -metrics :9191
```

### Docker Compose

```yaml
services:
  redis-proxy:
    image: ghcr.io/bootjp/elastickv/redis-proxy:latest
    ports:
      - "6379:6479"
      - "9191:9191"
    command:
      - -listen=:6479
      - -primary=redis:6379
      - -secondary=elastickv:6380
      - -mode=dual-write-shadow
      - -metrics=:9191
    depends_on:
      - redis
      - elastickv

  redis:
    image: redis:7
    ports:
      - "6379"

  elastickv:
    image: ghcr.io/bootjp/elastickv:latest
    ports:
      - "6380"
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-proxy
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis-proxy
  template:
    metadata:
      labels:
        app: redis-proxy
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9191"
    spec:
      containers:
        - name: redis-proxy
          image: ghcr.io/bootjp/elastickv/redis-proxy:latest
          args:
            - -listen=:6479
            - -primary=redis:6379
            - -secondary=elastickv:6380
            - -mode=dual-write-shadow
            - -metrics=:9191
          ports:
            - containerPort: 6479
              name: redis
            - containerPort: 9191
              name: metrics
          livenessProbe:
            tcpSocket:
              port: 6479
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            tcpSocket:
              port: 6479
            initialDelaySeconds: 3
            periodSeconds: 5
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: "1"
              memory: 512Mi
```

> **Note:** The distroless base image does not include `redis-cli`. If you want to use the `exec`-based probes above, build a redis-proxy image that includes `redis-cli` (or another ping tool) in the same container. Otherwise, prefer a `tcpSocket` probe (as below) or an HTTP health endpoint.

```yaml
# Alternative: TCP socket probe (no redis-cli needed)
livenessProbe:
  tcpSocket:
    port: 6479
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Health Checks

The proxy does not expose an HTTP health endpoint. Use the Redis `PING` command to verify availability:

```bash
redis-cli -p 6479 PING
# PONG
```

## Prometheus Metrics

Available at `/metrics` on the address specified by `-metrics`.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `proxy_command_total` | Counter | Commands processed (labels: command, backend, status) |
| `proxy_command_duration_seconds` | Histogram | Backend command latency |
| `proxy_primary_write_errors_total` | Counter | Primary write errors |
| `proxy_secondary_write_errors_total` | Counter | Secondary write errors |
| `proxy_primary_read_errors_total` | Counter | Primary read errors |
| `proxy_shadow_read_errors_total` | Counter | Shadow read errors |
| `proxy_divergences_total` | Counter | Shadow read mismatches (labels: command, kind) |
| `proxy_migration_gap_total` | Counter | Expected mismatches from incomplete migration (labels: command) |
| `proxy_async_drops_total` | Counter | Async operations dropped due to backpressure |
| `proxy_active_connections` | Gauge | Current active client connections |
| `proxy_pubsub_shadow_divergences_total` | Counter | Pub/Sub shadow message mismatches (labels: kind) |
| `proxy_pubsub_shadow_errors_total` | Counter | Pub/Sub shadow operation errors |

### Recommended Alerts

```yaml
groups:
  - name: redis-proxy
    rules:
      - alert: ProxyDivergenceHigh
        expr: rate(proxy_divergences_total[5m]) > 0
        for: 10m
        annotations:
          summary: "Data mismatch detected between primary and secondary"

      - alert: ProxySecondaryWriteErrors
        expr: rate(proxy_secondary_write_errors_total[5m]) > 1
        for: 5m
        annotations:
          summary: "Secondary backend write errors are elevated"

      - alert: ProxyAsyncDrops
        expr: rate(proxy_async_drops_total[5m]) > 0
        for: 5m
        annotations:
          summary: "Async goroutine limit reached; secondary may be slow"
```

## Internal Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| Connection pool size | 128 | go-redis pool size per backend |
| Dial timeout | 5s | Backend connection timeout |
| Read timeout | 3s | Backend read timeout |
| Write timeout | 3s | Backend write timeout |
| Async write goroutine limit | 4096 | Max concurrent secondary writes |
| Shadow read goroutine limit | 1024 | Max concurrent shadow comparisons |
| PubSub compare window | 2s | Message matching window |
| PubSub sweep interval | 500ms | Expired message scan interval |

## Graceful Shutdown

The proxy handles `SIGINT` / `SIGTERM` for graceful shutdown:

1. Stops accepting new connections
2. Waits for in-flight async goroutines to complete
3. Releases backend connection pools
4. Flushes Sentry buffers (up to 2 seconds)

Recommended shutdown order: `redis-proxy -> application -> Redis / ElasticKV`.

## Troubleshooting

### Secondary writes are falling behind
- Check `proxy_async_drops_total`. If increasing, the goroutine limit is being hit.
- Reduce `-secondary-timeout` to fail fast on slow secondaries.
- Investigate secondary (ElasticKV) performance.

### High divergence count
- Also check `proxy_migration_gap_total`. Pre-migration missing keys are counted as gaps, not divergences.
- In `dual-write-shadow` mode, inspect `proxy_divergences_total` labels to identify which commands are mismatched.

### Pub/Sub messages missing
- Check `proxy_pubsub_shadow_divergences_total`.
- `kind=data_mismatch`: message received by primary but not secondary.
- `kind=extra_data`: message received by secondary only.
