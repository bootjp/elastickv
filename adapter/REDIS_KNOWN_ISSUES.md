# Redis Adapter: Known Implementation Issues

This document tracks architectural limitations identified during PR #350 review
that require larger refactoring to address.

## Performance

### LPUSH / list mutations are O(N)
- **Location**: `redis_compat_commands.go` (lpush, ltrim, rpush)
- **Problem**: List modification commands read the entire list, modify in memory,
  and rewrite the whole list. This is O(N) where N is the number of elements.
- **Fix**: Push list manipulation down into the storage layer so individual
  elements can be appended/removed without full rewrite.

### SCAN materializes all keys
- **Location**: `redis_compat_commands.go` (scan)
- **Problem**: SCAN builds and sorts the full set of visible keys on every call,
  using the cursor as an array index. This is O(N) per call and diverges from
  Redis's incremental cursor semantics.
- **Fix**: Implement SCAN as an incremental range scan over the underlying store,
  returning the next cursor based on the last returned key.

### FLUSHDB iterates all keys
- **Location**: `redis_compat_commands.go` (flushdb)
- **Problem**: FLUSHDB/FLUSHALL iterates through all visible keys and deletes
  them one by one. This can be very slow for large datasets.
- **Fix**: Implement a range deletion capability at the storage layer.

### visibleKeys N x M per-key type checks
- **Location**: `redis_compat_helpers.go` (visibleKeys)
- **Problem**: `visibleKeys` calls `logicalExistsAt` per key, which performs
  multiple `ExistsAt` checks across namespaces (list, hash, set, zset, stream,
  ttl, etc.). For KEYS, DBSIZE, and SCAN this becomes O(N*M).
- **Fix**: Filter internal namespaces during scanning and derive logical
  existence in a single pass.

### localKeysPattern redundant scans
- **Location**: `redis.go` (localKeysPattern)
- **Problem**: `localKeysPattern` performs additional scans over list and Redis
  internal namespaces even when `start/end` are nil (full keyspace). This causes
  multiple redundant full scans per call.
- **Fix**: Only do extra namespace scans when the pattern has a bounded user
  prefix, or skip when `start == nil && end == nil`.

### BZPOPMIN / XREAD busy-polling
- **Location**: `redis_compat_commands.go` (bzpopmin, xread)
- **Problem**: Both use `time.Sleep` polling loops to wait for data. This wastes
  CPU cycles when there are no new elements.
- **Fix**: Implement a wait/notify pattern where ZADD/XADD signal waiting
  BZPOPMIN/XREAD goroutines via condition variables or channels.

## Consistency

### EXISTS / GET follower reads
- **Location**: `redis.go` (exists, get)
- **Problem**: EXISTS checks local MVCC state without leader proxying, which can
  return stale results on followers. GET's pre-check via `keyType()` also uses
  local state, potentially returning false negatives when the follower lags.
- **Fix**: Proxy EXISTS reads to the leader (or verify leader status), and
  perform the GET type check via the leader/proxy path as well.

## Design

### PFCOUNT is exact cardinality, not HyperLogLog
- **Location**: `redis_compat_commands.go` (pfadd, pfcount)
- **Problem**: PFADD/PFCOUNT stores all unique elements in a set and returns its
  exact size, rather than using a probabilistic HyperLogLog structure.
  For large datasets this consumes significantly more memory than a real HLL.
- **Note**: For Misskey's use case the exact implementation is acceptable. Only
  relevant if elastickv is used as a general-purpose Redis replacement with
  large cardinality sets.
