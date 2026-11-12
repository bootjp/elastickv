// Package clusterbootstrap implements protected Raft bootstrap for a server
// process and the offline `elastickv bootstrap ...` operator CLI: prepared
// peer configurations and baselines, the per-group bootstrap domain with its
// admission arbiter and startup token, the peer-session network, protected
// rotation and registration, the dedicated TSO group's protected start-up, the
// startup public-KV gate, and GroupRuntime — the node runtime core together
// with its protected-bootstrap attachments.
package clusterbootstrap
