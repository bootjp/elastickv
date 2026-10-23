package raftengine

// NamespaceConfigurationAware receives only committed/snapshot ConfState on
// the serial apply loop. Members must include all voters (including outgoing
// joint voters), learners, and learners-next, not just the election quorum.
// A watcher or Status() result cannot substitute for these ordered hooks.
type NamespaceConfigurationAware interface {
	RecordAppliedNamespaceConfiguration(index uint64, members []uint64) error
	RecoverNamespaceConfiguration(snapshotIndex uint64, members []uint64) error
}
