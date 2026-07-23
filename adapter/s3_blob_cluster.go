package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	s3BlobCapabilityRefreshInterval = 30 * time.Second
	s3BlobCapabilityPollTimeout     = 3 * time.Second
	s3BlobPeerRPCTimeout            = 30 * time.Second
)

// S3BlobReplica is a peer endpoint in the Raft group that owns a chunkblob.
type S3BlobReplica struct {
	NodeID   string
	Address  string
	Suffrage string
}

// S3BlobCluster combines capability negotiation and peer-local blob transfer.
// Implementations return current owning-group membership for every operation;
// callers calculate durability from that authoritative set.
type S3BlobCluster interface {
	S3BlobOffloadCapabilityChecker
	SelfNodeID() string
	ReplicasForChunk(ctx context.Context, chunkKey []byte) ([]S3BlobReplica, error)
	PushChunkBlob(ctx context.Context, replica S3BlobReplica, digest [s3ChunkBlobSHA256Bytes]byte, payload []byte, commitTS uint64) error
	FetchChunkBlob(ctx context.Context, replica S3BlobReplica, digest [s3ChunkBlobSHA256Bytes]byte) ([]byte, error)
	Close() error
}

// S3BlobLocalStoreResolver exposes this process's store for a chunkblob key.
// This bypasses leader routing because chunkblob rows are peer-local auxiliary
// state and never enter Raft.
type S3BlobLocalStoreResolver interface {
	LocalStoreForKey(key []byte) (store.MVCCStore, bool)
}

type grpcS3BlobCluster struct {
	selfNodeID string
	members    kv.RaftMembershipCoordinator
	adminToken string
	peerToken  string
	now        func() time.Time
	capTimeout time.Duration
	rpcTimeout time.Duration

	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
	capability  s3BlobCapabilityCache
	closeCalled bool
}

type s3BlobCapabilityCache struct {
	fingerprint string
	expiresAt   time.Time
	allSupport  bool
}

// NewGRPCS3BlobCluster constructs the production peer client. An empty token
// always fails capability negotiation because S3BlobFetch is peer-only and is
// never exposed by the insecure Admin mode.
func NewGRPCS3BlobCluster(selfNodeID string, members kv.RaftMembershipCoordinator, adminToken, peerToken string) S3BlobCluster {
	return &grpcS3BlobCluster{
		selfNodeID: strings.TrimSpace(selfNodeID),
		members:    members,
		adminToken: adminToken,
		peerToken:  peerToken,
		now:        time.Now,
		capTimeout: s3BlobCapabilityPollTimeout,
		rpcTimeout: s3BlobPeerRPCTimeout,
		conns:      map[string]*grpc.ClientConn{},
	}
}

func (c *grpcS3BlobCluster) SelfNodeID() string {
	if c == nil {
		return ""
	}
	return c.selfNodeID
}

func (c *grpcS3BlobCluster) ReplicasForChunk(ctx context.Context, chunkKey []byte) ([]S3BlobReplica, error) {
	if c == nil || c.members == nil {
		return nil, errors.New("s3 blob membership provider is not configured")
	}
	members, err := c.members.RaftMembersForKey(ctx, chunkKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	seen := make(map[string]struct{}, len(members))
	replicas := make([]S3BlobReplica, 0, len(members))
	for _, member := range members {
		nodeID := strings.TrimSpace(member.NodeID)
		address := strings.TrimSpace(member.Address)
		if nodeID == "" || address == "" {
			return nil, errors.New("s3 blob membership contains an empty node id or address")
		}
		if _, duplicate := seen[nodeID]; duplicate {
			return nil, errors.WithStack(errors.Newf("s3 blob membership contains duplicate node id %q", nodeID))
		}
		seen[nodeID] = struct{}{}
		replicas = append(replicas, S3BlobReplica{NodeID: nodeID, Address: address, Suffrage: member.Suffrage})
	}
	if len(replicas) == 0 {
		return nil, errors.New("s3 blob membership is empty")
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].NodeID < replicas[j].NodeID })
	return replicas, nil
}

func (c *grpcS3BlobCluster) AllPeersSupportS3BlobOffload(ctx context.Context) bool {
	if c == nil || c.adminToken == "" || c.peerToken == "" || c.members == nil || !S3BlobOffloadLocalCapability() {
		return false
	}
	members, err := c.members.RaftMembers(ctx)
	if err != nil {
		return false
	}
	replicas, err := s3BlobCapabilityReplicas(members)
	if err != nil {
		return false
	}
	fingerprint := s3BlobReplicaFingerprint(replicas)
	now := c.now()
	if supported, ok := c.cachedCapability(fingerprint, now); ok {
		return supported
	}
	allSupport := c.pollCapabilities(ctx, replicas)
	c.storeCapability(fingerprint, now.Add(s3BlobCapabilityRefreshInterval), allSupport)
	return allSupport
}

func s3BlobCapabilityReplicas(members []kv.RaftMember) ([]S3BlobReplica, error) {
	seen := make(map[string]struct{}, len(members))
	replicas := make([]S3BlobReplica, 0, len(members))
	for _, member := range members {
		nodeID := strings.TrimSpace(member.NodeID)
		address := strings.TrimSpace(member.Address)
		if nodeID == "" || address == "" {
			return nil, errors.New("s3 blob cluster membership contains an empty node id or address")
		}
		endpoint := nodeID + "\x00" + address
		if _, duplicate := seen[endpoint]; duplicate {
			continue
		}
		seen[endpoint] = struct{}{}
		replicas = append(replicas, S3BlobReplica{NodeID: nodeID, Address: address, Suffrage: member.Suffrage})
	}
	if len(replicas) == 0 {
		return nil, errors.New("s3 blob cluster membership is empty")
	}
	sort.Slice(replicas, func(i, j int) bool {
		if replicas[i].NodeID == replicas[j].NodeID {
			return replicas[i].Address < replicas[j].Address
		}
		return replicas[i].NodeID < replicas[j].NodeID
	})
	return replicas, nil
}

func s3BlobReplicaFingerprint(replicas []S3BlobReplica) string {
	var b strings.Builder
	for _, replica := range replicas {
		b.WriteString(replica.NodeID)
		b.WriteByte(0)
		b.WriteString(replica.Address)
		b.WriteByte(0)
		b.WriteString(replica.Suffrage)
		b.WriteByte('\n')
	}
	return b.String()
}

func (c *grpcS3BlobCluster) cachedCapability(fingerprint string, now time.Time) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capability.fingerprint != fingerprint || !now.Before(c.capability.expiresAt) {
		return false, false
	}
	return c.capability.allSupport, true
}

func (c *grpcS3BlobCluster) storeCapability(fingerprint string, expiresAt time.Time, supported bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capability = s3BlobCapabilityCache{fingerprint: fingerprint, expiresAt: expiresAt, allSupport: supported}
}

func (c *grpcS3BlobCluster) pollCapabilities(ctx context.Context, replicas []S3BlobReplica) bool {
	remote := make([]S3BlobReplica, 0, len(replicas))
	for _, replica := range replicas {
		if replica.NodeID != c.selfNodeID {
			remote = append(remote, replica)
		}
	}
	results := make(chan bool, len(remote))
	for _, replica := range remote {
		go func() {
			results <- c.peerSupportsS3BlobOffload(ctx, replica)
		}()
	}
	for range remote {
		if !<-results {
			return false
		}
	}
	return true
}

func (c *grpcS3BlobCluster) peerSupportsS3BlobOffload(ctx context.Context, replica S3BlobReplica) bool {
	conn, err := c.connFor(replica.Address)
	if err != nil {
		return false
	}
	timeout := c.capTimeout
	if timeout <= 0 {
		timeout = s3BlobCapabilityPollTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := pb.NewAdminClient(conn).GetClusterOverview(c.authorizedAdminContext(callCtx), &pb.GetClusterOverviewRequest{})
	return err == nil && resp.GetSelf().GetNodeId() == replica.NodeID && resp.GetCapabilities()[S3BlobOffloadCapabilityName]
}

func (c *grpcS3BlobCluster) PushChunkBlob(ctx context.Context, replica S3BlobReplica, digest [s3ChunkBlobSHA256Bytes]byte, payload []byte, commitTS uint64) error {
	if commitTS == 0 {
		return errors.New("s3 chunkblob push requires a commit timestamp")
	}
	conn, err := c.connFor(replica.Address)
	if err != nil {
		return err
	}
	callCtx, cancel := c.peerRPCContext(ctx)
	defer cancel()
	stream, err := pb.NewS3BlobFetchClient(conn).PushChunkBlob(c.authorizedPeerContext(callCtx))
	if err != nil {
		return errors.WithStack(err)
	}
	if err := sendS3ChunkBlobPushFrames(stream, digest, payload, commitTS); err != nil {
		return err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.GetDurable() {
		return errors.WithStack(status.Error(codes.DataLoss, "s3 chunkblob peer did not acknowledge durable storage"))
	}
	return nil
}

func sendS3ChunkBlobPushFrames(
	stream pb.S3BlobFetch_PushChunkBlobClient,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) error {
	for offset, first := 0, true; first || offset < len(payload); first = false {
		end := offset + s3BlobFetchFrameBytes
		if end > len(payload) {
			end = len(payload)
		}
		req := &pb.PushChunkBlobRequest{Payload: payload[offset:end], Eof: end == len(payload)}
		if first {
			req.ContentSha256 = digest[:]
			req.CommitTs = commitTS
		}
		if err := stream.Send(req); err != nil {
			return errors.WithStack(err)
		}
		offset = end
	}
	return nil
}

func (c *grpcS3BlobCluster) FetchChunkBlob(ctx context.Context, replica S3BlobReplica, digest [s3ChunkBlobSHA256Bytes]byte) ([]byte, error) {
	conn, err := c.connFor(replica.Address)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := c.peerRPCContext(ctx)
	defer cancel()
	stream, err := pb.NewS3BlobFetchClient(conn).FetchChunkBlob(c.authorizedPeerContext(callCtx), &pb.FetchChunkBlobRequest{ContentSha256: digest[:]})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	payload := make([]byte, 0, s3ChunkSize)
	seenEOF := false
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, errors.WithStack(recvErr)
		}
		if seenEOF {
			return nil, errors.WithStack(status.Error(codes.InvalidArgument, "s3 chunkblob fetch returned data after eof"))
		}
		if len(payload)+len(frame.GetPayload()) > s3ChunkSize {
			return nil, errors.WithStack(status.Error(codes.ResourceExhausted, "s3 chunkblob fetch exceeds chunk size"))
		}
		payload = append(payload, frame.GetPayload()...)
		seenEOF = frame.GetEof()
	}
	if !seenEOF {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "s3 chunkblob fetch omitted eof"))
	}
	actual := sha256.Sum256(payload)
	if !bytes.Equal(actual[:], digest[:]) {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "s3 chunkblob fetch sha256 mismatch"))
	}
	return payload, nil
}

func (c *grpcS3BlobCluster) peerRPCContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := c.rpcTimeout
	if timeout <= 0 {
		timeout = s3BlobPeerRPCTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *grpcS3BlobCluster) authorizedAdminContext(ctx context.Context) context.Context {
	if c == nil || c.adminToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.adminToken)
}

func (c *grpcS3BlobCluster) authorizedPeerContext(ctx context.Context) context.Context {
	if c == nil || c.peerToken == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.peerToken)
}

func (c *grpcS3BlobCluster) connFor(address string) (*grpc.ClientConn, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, errors.New("s3 blob peer address is empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeCalled {
		return nil, errors.New("s3 blob cluster is closed")
	}
	if conn := c.conns[address]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(address, internal.GRPCDialOptions()...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.conns[address] = conn
	return conn, nil
}

func (c *grpcS3BlobCluster) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closeCalled {
		c.mu.Unlock()
		return nil
	}
	c.closeCalled = true
	conns := make([]*grpc.ClientConn, 0, len(c.conns))
	for _, conn := range c.conns {
		conns = append(conns, conn)
	}
	clear(c.conns)
	c.mu.Unlock()
	var first error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && first == nil {
			first = errors.WithStack(err)
		}
	}
	return first
}
