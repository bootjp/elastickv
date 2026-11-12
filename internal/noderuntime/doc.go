// Package noderuntime resolves a node's Raft runtime configuration from
// parsed flag values: the Raft group and shard-range topology, the
// Raft-to-protocol leader address maps, the HT-FIFO partition map, and the
// bootstrap / join peer configuration. It is pure configuration and opens no
// stores or engines; the server binary passes the result down to the code
// that builds the shard groups.
package noderuntime
