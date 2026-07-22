package redislimits

// DefaultElasticKVRedisConnections is the shared default connection budget
// between the Redis proxy's ElasticKV pool and the Redis adapter's per-peer cap.
const DefaultElasticKVRedisConnections = 64
