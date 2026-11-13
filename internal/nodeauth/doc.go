// Package nodeauth configures how a server process authenticates gRPC
// traffic between nodes and toward its node-local Admin service: the bearer
// token files (admin, split migration, S3 blob peer) and their validation,
// the development-only insecure mode, peer gRPC TLS, the bearer-token
// interceptors, and the split-migration readiness gates that depend on
// peer-auth enforcement.
package nodeauth
