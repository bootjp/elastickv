package main

import (
	"flag"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/cockroachdb/errors"
)

var errInvalidLiveBackupConfig = errors.New("invalid live backup configuration")

// raftFlags identify this node in the Raft cluster: its address, Raft ID,
// engine and data directory.
type raftFlags struct {
	address string
	id      string
	engine  string
	dataDir string
}

func (f *raftFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.address, "address", "localhost:50051", "TCP host+port for this node")
	fs.StringVar(&f.id, "raftId", "", "Node id used by Raft")
	fs.StringVar(&f.engine, "raftEngine", string(noderuntime.RaftEngineEtcd), "Raft engine implementation (etcd)")
	fs.StringVar(&f.dataDir, "raftDataDir", "data/", "Raft data dir")
}

// topologyFlags describe the cluster layout: the Raft groups, the shard
// ranges they own, the Raft-to-protocol address maps used to proxy to a
// leader, and the HT-FIFO partition map.
//
// sqsFifoPartitionMap is distinct from sqsMap (which maps raftAddr=sqsAddr
// for the proxyToLeader endpoint resolution). Its grammar is
// `queue.fifo:N=group_0,...,group_{N-1}` with multiple queues separated by
// `;` (Phase 3.D §5). Leaving it empty means no FIFO queue is partitioned
// and the legacy single-partition layout applies to every queue. The §11
// PR 2 dormancy gate rejects PartitionCount > 1 on CreateQueue regardless of
// this flag, so populating it has no effect on production traffic until
// PR 5 of the rollout lands.
type topologyFlags struct {
	groups              string
	shardRanges         string
	redisMap            string
	s3Map               string
	dynamoMap           string
	sqsMap              string
	sqsFifoPartitionMap string
}

func (f *topologyFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.groups, "raftGroups", "", "Comma-separated raft groups (groupID=host:port,...)")
	fs.StringVar(&f.shardRanges, "shardRanges", "", "Comma-separated shard ranges (start:end=groupID,...)")
	fs.StringVar(&f.redisMap, "raftRedisMap", "", "Map of Raft address to Redis address (raftAddr=redisAddr,...)")
	fs.StringVar(&f.s3Map, "raftS3Map", "", "Map of Raft address to S3 address (raftAddr=s3Addr,...)")
	fs.StringVar(&f.dynamoMap, "raftDynamoMap", "", "Map of Raft address to DynamoDB address (raftAddr=dynamoAddr,...)")
	fs.StringVar(&f.sqsMap, "raftSqsMap", "", "Map of Raft address to SQS address (raftAddr=sqsAddr,...)")
	fs.StringVar(&f.sqsFifoPartitionMap, "sqsFifoPartitionMap", "", "HT-FIFO partition map (queue.fifo:N=group_0,...,group_{N-1};...)")
}

func resolveValidatedRuntimeInputs(flags *serverFlags) (noderuntime.RuntimeConfig, noderuntime.RaftEngineType, noderuntime.BootstrapConfig, bool, error) {
	if err := validateCommitTSFenceStartup(flags.consistency.commitTSFence, flags.bootstrap.peerConfigDir != ""); err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, err
	}
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveRuntimeInputs(flags.raft, flags.topology, flags.listeners, flags.bootstrap)
	if err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, err
	}
	backup := flags.backup
	err = validateLiveBackupConfig(
		backup.defaultTTL, backup.maxTTL, backup.beginDeadline,
		backup.snapshotHeadroomEntries, backup.scanPageSize, backup.maxActivePins,
	)
	return cfg, engineType, bootstrapCfg, bootstrap, err
}

func validateLiveBackupConfig(
	defaultTTL time.Duration,
	maxTTL time.Duration,
	beginDeadline time.Duration,
	snapshotHeadroom uint64,
	scanPageSize int,
	maxActivePins int,
) error {
	const minTTL = time.Minute
	switch {
	case defaultTTL < minTTL:
		return errors.Wrapf(errInvalidLiveBackupConfig, "backupDefaultTTL must be at least %s", minTTL)
	case maxTTL < minTTL:
		return errors.Wrapf(errInvalidLiveBackupConfig, "backupMaxTTL must be at least %s", minTTL)
	case defaultTTL > maxTTL:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupDefaultTTL must not exceed backupMaxTTL")
	case beginDeadline <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupBeginDeadline must be positive")
	case snapshotHeadroom == 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupSnapshotHeadroomEntries must be positive")
	case scanPageSize <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupScanPageSize must be positive")
	case maxActivePins <= 0:
		return errors.Wrap(errInvalidLiveBackupConfig, "backupMaxActivePins must be positive")
	default:
		return nil
	}
}

func resolveRuntimeInputs(raft raftFlags, topology topologyFlags, listeners listenerFlags, bootstrap raftBootstrapFlags) (noderuntime.RuntimeConfig, noderuntime.RaftEngineType, noderuntime.BootstrapConfig, bool, error) {
	if raft.id == "" {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.New("flag --raftId is required")
	}

	engineType, err := noderuntime.ParseRaftEngineType(raft.engine)
	if err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.WithStack(err)
	}

	cfg, err := noderuntime.ParseRuntimeConfig(raft.address, listeners.redisAddress, listeners.s3Address, listeners.dynamoAddress, listeners.sqsAddress,
		topology.groups, topology.shardRanges, topology.redisMap, topology.s3Map, topology.dynamoMap, topology.sqsMap, topology.sqsFifoPartitionMap)
	if err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.WithStack(err)
	}
	cfg.S3BlobBackfillConfig, err = adapter.S3BlobBackfillConfigFromEnv()
	if err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.WithStack(err)
	}

	bootstrapCfg, err := noderuntime.ResolveRaftPeerConfig(
		raft.id,
		cfg.Groups,
		bootstrap.bootstrapMembers,
		bootstrap.groupPeers,
		bootstrap.joinMembers,
		bootstrap.joinAsLearner,
		bootstrap.bootstrap,
	)
	if err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.WithStack(err)
	}
	bootstrapCfg.PeerConfigDir = bootstrap.peerConfigDir
	bootstrapCfg.JoinAsLearner = bootstrap.joinAsLearner

	return cfg, engineType, bootstrapCfg, bootstrap.bootstrap || bootstrapCfg.AnyBootstrapServers(), nil
}

func resolveNetworkRuntimeInputs(flags *serverFlags) (noderuntime.RuntimeConfig, noderuntime.RaftEngineType, noderuntime.BootstrapConfig, bool, error) {
	if err := nodeauth.ConfigureGRPCTransport(flags.nodeAuth); err != nil {
		return noderuntime.RuntimeConfig{}, "", noderuntime.BootstrapConfig{}, false, errors.WithStack(err)
	}
	cfg, engineType, bootstrapCfg, bootstrap, err := resolveValidatedRuntimeInputs(flags)
	bootstrapCfg.ServiceTokenFiles = flags.nodeAuth.ServiceTokenFiles()
	return cfg, engineType, bootstrapCfg, bootstrap, err
}
