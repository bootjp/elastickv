package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const multiEnablementMembers = 3

type multiEnablementIdentity struct {
	id, address, dir, configDir string
	raw                         bootstrapPeerJSON
	config                      *bootstrapPeerConfig
	listener                    net.Listener
}

type multiEnablementProcess struct {
	runtime     *raftGroupRuntime
	shard       *kv.ShardGroup
	domain      *bootstrapDomain
	coordinate  *kv.ShardedCoordinator
	shards      *kv.ShardStore
	transaction *kv.TransactionManager
	cleanup     internalutil.CleanupStack
	gate        *startupPublicKVGate
	server      *grpc.Server
	cancel      context.CancelFunc
	ctx         context.Context
	eg          *errgroup.Group
	served      chan error
	stopped     bool
}

func multiEnablementPEM(t *testing.T, path, kind string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0o600))
}

func multiEnablementIdentities(t *testing.T, base string) []*multiEnablementIdentity {
	t.Helper()
	pki := newBootstrapGatePKI(t)
	artifacts := t.TempDir()
	caPath := filepath.Join(artifacts, "ca.pem")
	multiEnablementPEM(t, caPath, "CERTIFICATE", pki.ca.Raw)
	tokenPath := filepath.Join(artifacts, "startup-token")
	require.NoError(t, os.WriteFile(tokenPath, []byte(bootstrapGateTestToken), 0o600))
	members := make([]bootstrapPeerMemberJSON, 0, multiEnablementMembers)
	identities := make([]*multiEnablementIdentity, 0, multiEnablementMembers)
	for i := range multiEnablementMembers {
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		id := fmt.Sprintf("n%d", i+1)
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), DNSNames: []string{"localhost"},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: pki.ca.NotBefore, NotAfter: pki.ca.NotAfter,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, pki.ca, pub, pki.key)
		require.NoError(t, err)
		parsed, err := x509.ParseCertificate(der)
		require.NoError(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		certPath, keyPath := filepath.Join(artifacts, id+".crt"), filepath.Join(artifacts, id+".key")
		multiEnablementPEM(t, certPath, "CERTIFICATE", der)
		multiEnablementPEM(t, keyPath, "PRIVATE KEY", keyDER)
		pin := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
		memberID := etcd.DeriveNodeID(id)
		members = append(members, bootstrapPeerMemberJSON{ID: memberID, Address: listener.Addr().String(), Key: hex.EncodeToString(pin[:]), Groups: []uint64{1}})
		identities = append(identities, &multiEnablementIdentity{id: id, address: listener.Addr().String(),
			dir: groupDataDir(base, id, 1, false), configDir: t.TempDir(), listener: listener,
			raw: bootstrapPeerJSON{Cluster: "12345678-1234-1234-1234-123456789abc", Local: memberID,
				CA: caPath, Cert: certPath, Key: keyPath, StartupTokenFile: tokenPath}})
	}
	for _, identity := range identities {
		identity.raw.Members = members
		multiEnablementWriteConfig(t, identity)
	}
	return identities
}

func multiEnablementWriteConfig(t *testing.T, identity *multiEnablementIdentity) {
	t.Helper()
	encoded, err := json.Marshal(identity.raw)
	require.NoError(t, err)
	path := filepath.Join(identity.configDir, "group-1.json")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	identity.config, err = loadBootstrapPeerConfig(path)
	require.NoError(t, err)
}

func multiEnablementStart(t *testing.T, base string, identities []*multiEnablementIdentity, protected bool, lagging ...bool) []*multiEnablementProcess {
	t.Helper()
	delayLastMember := len(lagging) != 0 && lagging[0]
	processes := make([]*multiEnablementProcess, 0, len(identities))
	var closeOnce sync.Once
	closeAll := func() { closeOnce.Do(func() { multiEnablementStop(t, processes) }) }
	t.Cleanup(closeAll)
	servers := make([]raftengine.Server, 0, len(identities))
	for _, identity := range identities {
		servers = append(servers, raftengine.Server{ID: identity.id, Address: identity.address, Suffrage: raftMemberSuffrageVoter})
	}
	for i, identity := range identities {
		if protected && delayLastMember && i == multiEnablementMembers-1 {
			multiEnablementQuorumSnapshot(t, processes)
		}
		process := multiEnablementStartMember(t, base, identity, servers, protected)
		processes = append(processes, process)
	}
	t.Cleanup(closeAll)
	return processes
}

func multiEnablementQuorumSnapshot(t *testing.T, processes []*multiEnablementProcess) {
	t.Helper()
	leader := multiEnablementLeader(t, processes)
	metadata, ok := leader.runtime.store.(store.RaftControlMetadataStore)
	require.True(t, ok)
	identity := raftbootstrap.Identity{Cluster: leader.runtime.bootstrapPeerConfig.local.Cluster, Group: 1}
	prepared, err := kv.ReadPreparedBootstrapActivation(t.Context(), metadata, identity)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return leader.runtime.engine.Status().LastSnapshotIndex > prepared.Probe.CommitIndex
	}, bootstrapE2EWaitTimeout, 10*time.Millisecond, "real quorum snapshot precedes the delayed prepared member's startup")
}

func multiEnablementStartMember(t *testing.T, base string, identity *multiEnablementIdentity, servers []raftengine.Server, protected bool) *multiEnablementProcess {
	t.Helper()
	group := groupSpec{id: 1, address: identity.address}
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, group.id)
	clock := kv.NewHLC()
	factory := etcd.NewFactory(etcd.FactoryConfig{TickInterval: 50 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 20})
	builder := shardGroupBuilder{raftID: identity.id, raftDir: base, bootstrap: !protected,
		factory: factory,
		clock:   clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes}
	if !protected {
		builder.bootstrapCfg = raftBootstrapConfig{legacyServers: servers}
	}
	previous := *raftBootstrapPeerConfigDir
	defer func() { *raftBootstrapPeerConfigDir = previous }()
	var domain *bootstrapDomain
	listener := identity.listener
	if protected {
		*raftBootstrapPeerConfigDir = identity.configDir
		var err error
		builder.bootstrapSpecs, domain, err = loadProtectedBootstrapDomain(identity.id, base, []groupSpec{group}, false, raftBootstrapConfig{})
		require.NoError(t, err)
		t.Cleanup(domain.admission.Close)
		loaded := builder.bootstrapSpecs[group.id].config
		require.Equal(t, identity.config.local, loaded.local)
		require.Equal(t, identity.config.provisions, loaded.provisions, "restart retains all original CA/SPKI provisions")
		listener, err = (&net.ListenConfig{}).Listen(t.Context(), "tcp", identity.address)
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
	}
	runtime, shard, err := builder.build(group)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	process := &multiEnablementProcess{runtime: runtime, shard: shard, domain: domain, served: make(chan error, 1)}
	process.ctx, process.cancel = context.WithCancel(t.Context())
	process.eg, process.ctx = errgroup.WithContext(process.ctx)
	options := append([]grpc.ServerOption{}, runtime.transportServerOptions...)
	if protected {
		options = append(options, multiEnablementNetwork(t, process, routes, clock)...)
	}
	process.server = grpc.NewServer(options...)
	runtime.registerGRPC(process.server)
	if protected {
		multiEnablementServices(process, clock)
	}
	go func() { process.served <- process.server.Serve(listener) }()
	return process
}

func multiEnablementNetwork(t *testing.T, process *multiEnablementProcess, routes *distribution.Engine, clock *kv.HLC) []grpc.ServerOption {
	t.Helper()
	id := process.runtime.spec.id
	groups := map[uint64]*kv.ShardGroup{id: process.shard}
	process.shards = kv.NewShardStore(routes, groups)
	process.shards.SetOperationAdmission(process.domain.operationAdmission)
	network, err := configureBootstrapNetwork(process.ctx, []*raftGroupRuntime{process.runtime}, process.shards, groups, &process.cleanup)
	require.NoError(t, err)
	require.NotNil(t, network, "ordinary forwarding must use the actual runtime-scoped pinned TLS factory")
	process.coordinate = kv.NewShardedCoordinator(routes, groups, id, clock, process.shards).WithOperationAdmission(process.domain.operationAdmission)
	catalog := distributionCatalogStoreForGroup([]*raftGroupRuntime{process.runtime}, id)
	startBootstrapReadinessReconciler(process.ctx, process.eg, []*raftGroupRuntime{process.runtime}, catalog, routes)
	process.gate = multiEnablementGate(process)
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(process.gate.unaryInterceptor), grpc.ChainStreamInterceptor(process.gate.streamInterceptor)}
}

func multiEnablementGate(process *multiEnablementProcess) *startupPublicKVGate {
	gate := &startupPublicKVGate{domain: process.domain, blockRuntime: func() bool { return !process.runtime.publicReady() }}
	gate.markReady()
	return gate
}

func multiEnablementServices(process *multiEnablementProcess, clock *kv.HLC) {
	coordinate := newStartupGatedCoordinator(process.coordinate, process.gate)
	service := adapter.NewGRPCServer(process.shards, coordinate)
	pb.RegisterRawKVServer(process.server, service)
	pb.RegisterTransactionalKVServer(process.server, service)
	proposer := process.shard.Proposer()
	process.transaction = newInternalMigrationTransaction(proposer, nil, process.runtime.store, process.runtime.spec.id)
	options := internalServerOptions(coordinate, nil, proposer, process.shard, process.runtime.spec.id)
	options = append(options, adapter.WithInternalStore(process.runtime.store), adapter.WithInternalPrimaryCommitEvidence(process.shards))
	pb.RegisterInternalServer(process.server, adapter.NewInternalWithEngine(process.transaction, process.runtime.engine, clock, nil, options...))
}

func multiEnablementStop(t *testing.T, processes []*multiEnablementProcess) {
	t.Helper()
	active := make([]*multiEnablementProcess, 0, len(processes))
	for _, process := range processes {
		if process.stopped {
			continue
		}
		process.stopped = true
		process.cancel()
		active = append(active, process)
	}
	var failures []error
	for _, process := range active {
		failures = append(failures, process.eg.Wait())
		failures = append(failures, process.closeReceivers())
	}
	// Stop all RawNodes together: sequential WAL/store closes must not permit a
	// surviving quorum to elect and advance the agreed quiescent baseline cut.
	var wg sync.WaitGroup
	engineErrors := make([]error, len(active))
	for i, process := range active {
		engine := process.runtime.snapshotEngine()
		if engine != nil {
			wg.Add(1)
			go func() { defer wg.Done(); engineErrors[i] = engine.Close() }()
		}
	}
	wg.Wait()
	for _, process := range active {
		process.runtime.Close()
		if process.domain != nil {
			process.domain.admission.Close()
		}
	}
	require.NoError(t, errors.Join(append(failures, engineErrors...)...))
}

func (p *multiEnablementProcess) closeReceivers() error {
	p.cleanup.Run()
	var failures []error
	if p.coordinate != nil {
		failures = append(failures, p.coordinate.Close())
	}
	if p.transaction != nil {
		p.transaction.Close()
	}
	if p.shards != nil {
		failures = append(failures, p.shards.Close())
	}
	p.server.Stop()
	return errors.Join(append(failures, <-p.served)...)
}

func multiEnablementLeader(t *testing.T, processes []*multiEnablementProcess) *multiEnablementProcess {
	t.Helper()
	var leader *multiEnablementProcess
	require.Eventually(t, func() bool {
		count := 0
		for _, process := range processes {
			if process.runtime.engine.State() == raftengine.StateLeader {
				leader = process
				count++
			}
		}
		return count == 1
	}, bootstrapE2EWaitTimeout, 10*time.Millisecond)
	return leader
}

func multiEnablementPopulate(t *testing.T, processes []*multiEnablementProcess) {
	t.Helper()
	leader := multiEnablementLeader(t, processes)
	clock := kv.NewHLC()
	enablementLegacyPut(t, leader.runtime.engine, clock.Next(), "legacy-key", "replicated-original")
	route, err := distribution.EncodeRouteDescriptor(distribution.RouteDescriptor{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive})
	require.NoError(t, err)
	start, commit := clock.Next(), clock.Next()
	payload, err := proto.Marshal(&pb.Request{IsTxn: true, Ts: start, ReadKeys: [][]byte{distribution.CatalogVersionKey()}, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: distribution.CatalogVersionKey(), CommitTS: commit})},
		{Op: pb.Op_PUT, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(1)},
		{Op: pb.Op_PUT, Key: distribution.CatalogNextRouteIDKey(), Value: distribution.EncodeCatalogNextRouteID(2)},
		{Op: pb.Op_PUT, Key: distribution.CatalogRouteKey(1), Value: route},
	}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancel()
	result, err := leader.runtime.engine.Propose(ctx, payload)
	require.NoError(t, err)
	if applyErr, ok := result.Response.(error); ok {
		require.NoError(t, applyErr, "native commit must also have successful FSM effects")
	}
	for _, process := range processes {
		require.Eventually(t, func() bool {
			status := process.runtime.engine.Status()
			return status.AppliedIndex == result.CommitIndex && status.CommitIndex == result.CommitIndex && status.LastSnapshotIndex >= result.CommitIndex
		}, bootstrapE2EWaitTimeout, 10*time.Millisecond)
		catalog := distributionCatalogStoreForGroup([]*raftGroupRuntime{process.runtime}, 1)
		snapshot, err := catalog.Snapshot(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(1), snapshot.Version, "the original cut contains a replicated catalog")
		require.Len(t, snapshot.Routes, 1)
		value, err := process.runtime.store.GetAt(t.Context(), []byte("legacy-key"), ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, "replicated-original", string(value))
	}
}

func multiEnablementBaseline(t *testing.T, identities []*multiEnablementIdentity) raftbootstrap.BaselineManifest {
	t.Helper()
	var manifest raftbootstrap.BaselineManifest
	for _, identity := range identities {
		probe := multiEnablementBaselineProbe(t, identity, manifest.BaselineContext, identities)
		if len(manifest.Members) == 0 {
			manifest.BaselineContext = probe.BaselineContext
		}
		manifest.Members = append(manifest.Members, probe.Evidence)
	}
	require.NoError(t, raftbootstrap.VerifyBaselineManifest(manifest, manifest.BaselineContext))
	return manifest
}

func multiEnablementBaselineProbe(t *testing.T, identity *multiEnablementIdentity, cut raftbootstrap.BaselineContext, identities []*multiEnablementIdentity) raftbootstrap.BaselineProbe {
	t.Helper()
	ownership, err := etcd.AcquireOfflineBaselineOwnership(identity.dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	source, err := etcd.ReadOfflineBaselineWALSource(t.Context(), identity.dir, ownership)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Snapshot.Close()) }()
	proof, err := bootstrapOfflineDataStoreProbe(t.Context(), identity.dir)
	require.NoError(t, err)
	state := raftbootstrap.ConfState{Voters: source.Configuration.GetVoters(), VotersOutgoing: source.Configuration.GetVotersOutgoing(),
		Learners: source.Configuration.GetLearners(), LearnersNext: source.Configuration.GetLearnersNext(), AutoLeave: source.Configuration.GetAutoLeave()}
	if cut.Version == 0 {
		cut = raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion, Identity: raftbootstrap.Identity{Cluster: identity.config.local.Cluster, Group: 1},
			OperationID: raftbootstrap.OperationID{72}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
			CommitIndex: source.CommitIndex, CommitTerm: source.CommitTerm, Watermark: proof.Watermark, DataDigest: proof.DataDigest,
			Configuration: raftbootstrap.Configuration{Index: source.ConfigurationIndex, State: state}}
		for _, member := range identities {
			cut.Configuration.Members = append(cut.Configuration.Members, raftbootstrap.Member{ID: member.config.local.Member,
				Address: member.address, PublicKeyDigest: member.config.local.Key})
		}
	}
	require.Equal(t, cut.CommitIndex, source.CommitIndex)
	require.Equal(t, cut.CommitIndex, source.LastLogIndex)
	require.Equal(t, cut.CommitIndex, proof.AppliedIndex)
	require.Equal(t, cut.CommitTerm, source.CommitTerm)
	require.Equal(t, cut.Configuration.Index, source.ConfigurationIndex)
	require.Equal(t, cut.Configuration.State, state)
	require.Equal(t, cut.Watermark, proof.Watermark)
	require.Equal(t, cut.DataDigest, proof.DataDigest)
	inspection, err := etcd.InspectOfflineBaseline(t.Context(), etcd.OfflineBaselineOptions{Ownership: ownership, DataDir: identity.dir,
		MemberID: identity.config.local.Member, Expected: cut, ProbeStore: func(ctx context.Context) (etcd.BaselineStoreProof, error) {
			return bootstrapOfflineDataStoreProbe(ctx, identity.dir)
		}})
	require.NoError(t, err)
	signer, ok := identity.config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: cut, Evidence: inspection.Evidence}, signer)
	require.NoError(t, err)
	return probe
}

func multiEnablementPrepareAck(t *testing.T, identity *multiEnablementIdentity, manifest raftbootstrap.BaselineManifest) []byte {
	t.Helper()
	original := offlineDataFiles(t, identity.dir)
	request := bootstrapBaselineCLIRequest{Command: "prepare", RaftDataDir: identity.dir, MemberID: identity.config.local.Member, OperationID: manifest.OperationID}
	prepared, err := executeBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	require.Equal(t, original, offlineDataFiles(t, prepared.BackupDir))
	request.Command = "resume"
	bound, err := executeBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", bound.Phase)
	require.Equal(t, original, offlineDataFiles(t, bound.BackupDir))
	ownership, err := etcd.AcquireOfflineBaselineOwnership(identity.dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	options, err := bootstrapOfflineOptions(request, manifest.BaselineContext, manifest, ownership)
	require.NoError(t, err)
	evidence, err := etcd.VerifyOfflineBaselinePrepared(t.Context(), options, manifest)
	require.NoError(t, err)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	require.Equal(t, digest, evidence.Identity.PreparationDigest)
	ack, err := signBootstrapPreparedAck(identity.config, manifest, identity.config.local.Member, evidence.DurableProofDigest)
	require.NoError(t, err)
	encoded, err := marshalBootstrapPreparedAck(ack, manifest)
	require.NoError(t, err)
	return encoded
}

func multiEnablementNoPolicy(t *testing.T, identity *multiEnablementIdentity, group raftbootstrap.Identity) {
	t.Helper()
	key, err := kv.BootstrapPolicyMetadataKey(group)
	require.NoError(t, err)
	err = bootstrapOfflineWithData(t.Context(), identity.dir, true, func(db *pebble.DB) error {
		snapshot := db.NewSnapshot()
		defer snapshot.Close()
		_, present, err := bootstrapOfflineGet(snapshot, append([]byte(store.RaftControlMetadataPrefix), key...))
		if present {
			return errors.New("P appeared before complete readiness certificate activation")
		}
		return err
	})
	require.NoError(t, err)
}

func multiEnablementReplicatedRead(t *testing.T, process *multiEnablementProcess, key, value string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, op, err := process.domain.admission.Begin(t.Context(), raftadmission.Data)
		if err != nil {
			return false
		}
		defer op.CancelAndJoin()
		got, err := process.runtime.store.GetAt(ctx, []byte(key), ^uint64(0))
		return finishBootstrapIngress(op, err) == nil && bytes.Equal(got, []byte(value))
	}, bootstrapE2EWaitTimeout, 10*time.Millisecond, "read each member's own Pebble, not a forwarded leader result")
}

func TestBootstrapEnablementThreeMembersAllReadyTLS(t *testing.T) {
	multiEnablementRun(t, false)
}

func TestBootstrapEnablementThreeMembersPreparedSnapshotCatchup(t *testing.T) {
	multiEnablementRun(t, true)
}

func multiEnablementRun(t *testing.T, lagging bool) {
	t.Helper()
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	require.Empty(t, *raftBootstrapPeerConfigDir)
	base := t.TempDir()
	identities := multiEnablementIdentities(t, base)
	legacy := multiEnablementStart(t, base, identities, false, false)
	multiEnablementPopulate(t, legacy)
	multiEnablementStop(t, legacy)
	manifest := multiEnablementBaseline(t, identities)
	acks := make([][]byte, 0, len(identities))
	for _, identity := range identities {
		acks = append(acks, multiEnablementPrepareAck(t, identity, manifest))
	}
	_, err := assembleBootstrapActivation(manifest, manifest.BaselineContext, acks[:2])
	require.Error(t, err, "a prepared quorum cannot replace the third member's final ACK")
	payload, err := assembleBootstrapActivation(manifest, manifest.BaselineContext, acks)
	require.NoError(t, err)
	command, matched, err := raftbootstrap.ParseActivationCommand(payload)
	require.NoError(t, err)
	require.True(t, matched)
	incomplete := command
	incomplete.Readiness.Members = incomplete.Readiness.Members[:2]
	_, err = raftbootstrap.EncodeActivationCommand(incomplete)
	require.Error(t, err, "baseline signatures must not substitute for the missing readiness ACK")
	activationPath := filepath.Join(t.TempDir(), "activation.bin")
	require.NoError(t, os.WriteFile(activationPath, payload, 0o600))
	for _, identity := range identities {
		multiEnablementNoPolicy(t, identity, manifest.Identity)
		identity.raw.ActivationFile = activationPath
		multiEnablementWriteConfig(t, identity)
	}
	protected := multiEnablementStart(t, base, identities, true, lagging)
	if lagging {
		source := multiEnablementSnapshotPreparation(t, protected[2], manifest)
		multiEnablementStop(t, protected)
		protected = multiEnablementStart(t, base, identities, true)
		require.Equal(t, source, multiEnablementSnapshotPreparation(t, protected[2], manifest),
			"cold pre-P recovery must retain the signed source J, not rewrite it as local evidence")
	}
	leader := multiEnablementLeader(t, protected)
	for _, process := range protected {
		_, err := kv.LoadAppliedBootstrapPolicy(t.Context(), process.runtime.store, manifest.Identity, process.runtime.engine.Status().AppliedIndex)
		require.ErrorIs(t, err, store.ErrKeyNotFound, "no activation worker has started")
	}
	startBootstrapActivationWorkers(leader.ctx, leader.eg, []*raftGroupRuntime{leader.runtime}, map[uint64]*kv.ShardGroup{1: leader.shard})
	activation := multiEnablementAssertActivePolicy(t, protected, manifest)
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancel()
	_, err = leader.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("protected-key"), Value: []byte("replicated-protected")}}})
	require.NoError(t, err)
	for _, process := range protected {
		multiEnablementReplicatedRead(t, process, "protected-key", "replicated-protected")
	}
	multiEnablementFollowerForward(t, protected, leader)
	if lagging {
		multiEnablementActiveSnapshotRestart(t, base, identities, protected, manifest, activation)
	}
}

func multiEnablementAssertActivePolicy(t *testing.T, protected []*multiEnablementProcess, manifest raftbootstrap.BaselineManifest) uint64 {
	t.Helper()
	var activation uint64
	for _, process := range protected {
		if !assert.Eventually(t, process.runtime.publicReady, bootstrapE2EWaitTimeout, 10*time.Millisecond) {
			multiEnablementReadinessDiagnostics(t, protected, manifest)
			t.FailNow()
		}
		require.Equal(t, peersession.DataReady, process.domain.admission.Stage(1))
		policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), process.runtime.store, manifest.Identity, process.runtime.engine.Status().AppliedIndex)
		require.NoError(t, err)
		if activation == 0 {
			activation = policy.ActivationIndex
		}
		require.Equal(t, activation, policy.ActivationIndex)
		require.Greater(t, policy.ActivationIndex, manifest.CommitIndex)
		require.Equal(t, uint64(1), policy.Generation)
		multiEnablementReplicatedRead(t, process, "legacy-key", "replicated-original")
	}
	return activation
}

func multiEnablementActiveSnapshotRestart(t *testing.T, base string, identities []*multiEnablementIdentity, protected []*multiEnablementProcess, manifest raftbootstrap.BaselineManifest, activation uint64) {
	t.Helper()
	source := multiEnablementSnapshotPreparation(t, protected[2], manifest)
	for _, process := range protected {
		require.Eventually(t, func() bool {
			return process.runtime.engine.Status().LastSnapshotIndex >= activation
		}, bootstrapE2EWaitTimeout, 10*time.Millisecond, "persist active P in real native snapshots before cold restart")
	}
	multiEnablementStop(t, protected)
	protected = multiEnablementStart(t, base, identities, true)
	require.Equal(t, source, multiEnablementSnapshotPreparation(t, protected[2], manifest))
	for _, process := range protected {
		require.Eventually(t, process.runtime.publicReady, bootstrapE2EWaitTimeout, 10*time.Millisecond)
		policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), process.runtime.store, manifest.Identity, process.runtime.engine.Status().AppliedIndex)
		require.NoError(t, err)
		require.Equal(t, activation, policy.ActivationIndex, "cold recovery cannot downgrade or reactivate P")
		require.Equal(t, uint64(1), policy.Generation)
		multiEnablementReplicatedRead(t, process, "legacy-key", "replicated-original")
		multiEnablementReplicatedRead(t, process, "protected-key", "replicated-protected")
		multiEnablementReplicatedRead(t, process, "forwarded-key", "tls-follower-forwarded")
	}
}

func multiEnablementSnapshotPreparation(t *testing.T, process *multiEnablementProcess, manifest raftbootstrap.BaselineManifest) kv.PreparedBootstrapActivation {
	t.Helper()
	metadata, ok := process.runtime.store.(store.RaftControlMetadataStore)
	require.True(t, ok)
	var prepared kv.PreparedBootstrapActivation
	require.Eventually(t, func() bool {
		var err error
		prepared, err = kv.ReadPreparedBootstrapActivation(t.Context(), metadata, manifest.Identity)
		return err == nil && prepared.Probe.Evidence.MemberID != process.runtime.bootstrapPeerConfig.local.Member &&
			process.runtime.engine.Status().LastSnapshotIndex > manifest.CommitIndex
	}, bootstrapE2EWaitTimeout, 10*time.Millisecond, "receiver must actually import another member's signed J via native snapshot")
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	require.Equal(t, digest, prepared.ManifestDigest)
	return prepared
}

func multiEnablementReadinessDiagnostics(t *testing.T, processes []*multiEnablementProcess, manifest raftbootstrap.BaselineManifest) {
	t.Helper()
	expected, digestErr := raftbootstrap.ConfigurationDigest(manifest.Configuration)
	for _, process := range processes {
		metadata, ok := process.runtime.store.(store.RaftControlMetadataStore)
		if !ok {
			t.Logf("member=%d lacks metadata reader", process.runtime.bootstrapPeerConfig.local.Member)
			continue
		}
		prepared, prepErr := kv.ReadPreparedBootstrapActivation(t.Context(), metadata, manifest.Identity)
		configuration, confErr := raftbootstrap.ConfigurationDigest(prepared.Probe.Configuration)
		policy, policyErr := kv.LoadAppliedBootstrapPolicy(t.Context(), process.runtime.store, manifest.Identity, process.runtime.engine.Status().AppliedIndex)
		t.Logf("member=%d stored_prepared_member=%d prepared_identity=%+v stage=%v native=%+v prep_error=%v configuration=%x expected_configuration=%x conf_error=%v digest_error=%v P_activation=%d P_error=%v",
			process.runtime.bootstrapPeerConfig.local.Member, prepared.Probe.Evidence.MemberID, prepared.Probe.Identity,
			process.domain.admission.Stage(1), process.runtime.engine.Status(), prepErr, configuration, expected, confErr, digestErr, policy.ActivationIndex, policyErr)
	}
}

func multiEnablementFollowerForward(t *testing.T, processes []*multiEnablementProcess, leader *multiEnablementProcess) {
	t.Helper()
	var follower *multiEnablementProcess
	for _, process := range processes {
		if process != leader {
			follower = process
			break
		}
	}
	require.NotNil(t, follower)
	require.Equal(t, raftengine.StateFollower, follower.runtime.engine.State())
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancel()
	_, err := follower.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put,
		Key: []byte("forwarded-key"), Value: []byte("tls-follower-forwarded")}}})
	require.NoError(t, err, "ordinary forwarding must retain receiver ownership on both nodes and use pinned TLS")
	// The leader ACK can precede follower snapshot restore, which revokes H.
	// Wait for local catch-up before acquiring ownership for the single TLS read.
	for _, process := range processes {
		multiEnablementReplicatedRead(t, process, "forwarded-key", "tls-follower-forwarded")
	}
	readCtx, cancelRead := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancelRead()
	owned, op, err := follower.domain.admission.Begin(readCtx, raftadmission.Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	value, err := follower.shards.GetAt(owned, []byte("forwarded-key"), ^uint64(0))
	err = finishBootstrapIngress(op, err)
	require.NoError(t, err, "follower read forwarding uses the same runtime-local ordinary TLS capability")
	require.Equal(t, "tls-follower-forwarded", string(value))
}
