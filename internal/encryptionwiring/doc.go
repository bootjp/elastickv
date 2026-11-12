// Package encryptionwiring wires data-at-rest encryption into a server
// process: the encryption flags, KEK loading and the start-up guards that
// refuse a node whose sidecar, KEK or Raft log disagree, the storage-envelope
// write wiring and the raft-envelope runtime, writer registration and its
// process-start gate, rotate-on-startup, the EncryptionAdmin service with its
// capability fan-out, and the membership-change interceptor. It reads Raft
// groups through noderuntime.GroupRuntime cores; protected bootstrap installs
// its writer-registration source through the context.
package encryptionwiring
