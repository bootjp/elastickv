package backup

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

type fakeLiveBackupRPC struct {
	mu sync.Mutex

	begin   *pb.BeginBackupResponse
	listed  []*pb.BackupScope
	records []*pb.BackupKV

	renewErr        error
	renewStarted    chan struct{}
	renewStartOnce  sync.Once
	renewWait       bool
	manifestPath    string
	manifestSeen    bool
	blockStream     bool
	streamRelease   <-chan struct{}
	streamStarted   chan struct{}
	streamStartOnce sync.Once
	renewCalls      int
	endCalls        int
	endedWithToken  []byte
}

func (f *fakeLiveBackupRPC) BeginBackup(context.Context, *pb.BeginBackupRequest) (*pb.BeginBackupResponse, error) {
	return f.begin, nil
}

func (f *fakeLiveBackupRPC) RenewBackup(ctx context.Context, _ *pb.RenewBackupRequest) (*pb.RenewBackupResponse, error) {
	f.mu.Lock()
	f.renewCalls++
	renewErr := f.renewErr
	renewWait := f.renewWait
	manifestPath := f.manifestPath
	f.mu.Unlock()
	if f.renewStarted != nil {
		f.renewStartOnce.Do(func() { close(f.renewStarted) })
	}
	if renewWait {
		<-ctx.Done()
		_, statErr := os.Lstat(manifestPath)
		f.mu.Lock()
		f.manifestSeen = statErr == nil
		f.mu.Unlock()
		return nil, ctx.Err()
	}
	if renewErr != nil {
		return nil, renewErr
	}
	return &pb.RenewBackupResponse{PinToken: []byte("renewed-token"), TtlMsEffective: f.begin.GetTtlMsEffective()}, nil
}

func (f *fakeLiveBackupRPC) EndBackup(_ context.Context, req *pb.EndBackupRequest) (*pb.EndBackupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.endCalls++
	f.endedWithToken = append([]byte(nil), req.GetPinToken()...)
	return &pb.EndBackupResponse{}, nil
}

func (f *fakeLiveBackupRPC) ListAdaptersAndScopes(context.Context, *pb.ListAdaptersAndScopesRequest) (*pb.ListAdaptersAndScopesResponse, error) {
	return &pb.ListAdaptersAndScopesResponse{Scopes: f.listed}, nil
}

func (f *fakeLiveBackupRPC) StreamBackup(ctx context.Context, _ *pb.StreamBackupRequest, consume func(*pb.BackupKV) error) error {
	if f.streamStarted != nil {
		f.streamStartOnce.Do(func() { close(f.streamStarted) })
	}
	if f.streamRelease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.streamRelease:
		}
	}
	if f.blockStream {
		<-ctx.Done()
		return ctx.Err()
	}
	for _, record := range f.records {
		if err := consume(record); err != nil {
			return err
		}
	}
	return nil
}

func successfulLiveBackupRPC() *fakeLiveBackupRPC {
	return &fakeLiveBackupRPC{
		begin: &pb.BeginBackupResponse{
			ReadTs: 42, PinToken: []byte("pin-token"), TtlMsEffective: 60_000, MaxActiveBackupPins: 4,
			Shards:       []*pb.BackupShardApplied{{RaftGroupId: 1, AppliedIndex: 99}},
			ExpectedKeys: []*pb.BackupExpectedKeys{{Adapter: "redis", Scope: "db_0", KeyCount: 1}},
		},
		listed:  []*pb.BackupScope{{Adapter: "redis", Scope: "db_0"}},
		records: []*pb.BackupKV{{Key: []byte(RedisStringPrefix + "hello"), Value: []byte("world")}},
	}
}

func TestRunLiveBackupFinalizesAndReleasesPin(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	root := filepath.Join(t.TempDir(), "dump")
	result, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
		ElastickvVersion: "test", Now: func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
	if err != nil {
		t.Fatalf("RunLiveBackup: %v", err)
	}
	if result.ReadTS != 42 || len(result.Scopes) != 1 {
		t.Fatalf("result=%+v", result)
	}
	assertLiveBackupManifest(t, root)
	assertLiveBackupEnded(t, rpc, "pin-token")
}

func assertLiveBackupManifest(t *testing.T, root string) {
	t.Helper()
	manifest := readLiveBackupManifest(t, root)
	if manifest.Live == nil || manifest.Live.ReadTS != 42 || manifest.BackupTSTTLMS != 60_000 || manifest.MaxActiveBackupPins != 4 {
		t.Fatalf("manifest live metadata=%+v", manifest)
	}
	if manifest.Adapters.Redis == nil || len(manifest.Adapters.Redis.Databases) != 1 || manifest.Adapters.Redis.Databases[0] != 0 {
		t.Fatalf("manifest adapters=%+v", manifest.Adapters)
	}
}

func readLiveBackupManifest(t *testing.T, root string) Manifest {
	t.Helper()
	if err := VerifyChecksums(root); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
	}
	manifestFile, err := os.Open(filepath.Join(root, ManifestFilename)) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("Open manifest: %v", err)
	}
	manifest, readErr := ReadManifest(manifestFile)
	closeErr := manifestFile.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("ReadManifest=%v Close=%v", readErr, closeErr)
	}
	return manifest
}

func assertLiveBackupEnded(t *testing.T, rpc *fakeLiveBackupRPC, token string) {
	t.Helper()
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if rpc.endCalls != 1 || string(rpc.endedWithToken) != token {
		t.Fatalf("endCalls=%d token=%q", rpc.endCalls, rpc.endedWithToken)
	}
}

func TestRunLiveBackupCountShortfallLeavesNoManifest(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.records = nil
	rpc.begin.ExpectedKeys[0].KeyCount = 1000
	root := filepath.Join(t.TempDir(), "dump")
	_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
	})
	if !errors.Is(err, ErrCompactionDuringDump) {
		t.Fatalf("err=%v, want ErrCompactionDuringDump", err)
	}
	if _, statErr := os.Lstat(filepath.Join(root, ManifestFilename)); !os.IsNotExist(statErr) {
		t.Fatalf("manifest exists after count shortfall: %v", statErr)
	}
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if rpc.endCalls != 1 {
		t.Fatalf("endCalls=%d, want 1", rpc.endCalls)
	}
}

func TestRunLiveBackupRenewalFailureCancelsStreamAndReleasesPin(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.begin.TtlMsEffective = 9
	rpc.renewErr = errors.New("leader election")
	rpc.blockStream = true
	root := filepath.Join(t.TempDir(), "dump")
	warnings := make(chan string, 1)
	_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
		WarnSink: func(event string, _ ...any) {
			warnings <- event
		},
	})
	if !errors.Is(err, ErrLiveBackupRenewal) {
		t.Fatalf("err=%v, want ErrLiveBackupRenewal", err)
	}
	if _, statErr := os.Lstat(filepath.Join(root, ManifestFilename)); !os.IsNotExist(statErr) {
		t.Fatalf("manifest exists after renewal failure: %v", statErr)
	}
	if event := <-warnings; event != "backup_pin_renewal_failed" {
		t.Fatalf("warning event = %q", event)
	}
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	if rpc.renewCalls == 0 || rpc.endCalls != 1 {
		t.Fatalf("renewCalls=%d endCalls=%d", rpc.renewCalls, rpc.endCalls)
	}
}

func TestRunLiveBackupStopsRenewalBeforeManifestPublication(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.begin.TtlMsEffective = 9
	rpc.renewStarted = make(chan struct{})
	rpc.renewWait = true
	rpc.streamRelease = rpc.renewStarted
	root := filepath.Join(t.TempDir(), "dump")
	rpc.manifestPath = filepath.Join(root, ManifestFilename)

	_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("RunLiveBackup: %v", err)
	}
	rpc.mu.Lock()
	manifestSeen := rpc.manifestSeen
	rpc.mu.Unlock()
	if manifestSeen {
		t.Fatal("MANIFEST.json was published while a pin renewal was still active")
	}
	manifest := readLiveBackupManifest(t, root)
	if manifest.Live == nil || manifest.Live.ReadTS != 42 || manifest.BackupTSTTLMS != 9 {
		t.Fatalf("manifest live metadata = %+v", manifest)
	}
}

func TestProducerCrashLeavesNoManifest(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.blockStream = true
	rpc.streamStarted = make(chan struct{})
	root := filepath.Join(t.TempDir(), "dump")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	warnings := make(chan string, 1)
	go func() {
		_, err := RunLiveBackup(ctx, rpc, LiveBackupOptions{
			OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
			WarnSink: func(event string, _ ...any) { warnings <- event },
		})
		done <- err
	}()
	<-rpc.streamStarted
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunLiveBackup error = %v, want context.Canceled", err)
	}
	assertLiveBackupHasNoManifest(t, root)
	assertLiveBackupEnded(t, rpc, "pin-token")
	select {
	case event := <-warnings:
		t.Fatalf("graceful cancellation emitted warning %q", event)
	default:
	}
}

func TestRunLiveBackupUsesRenewedTokenForCleanup(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.begin.TtlMsEffective = 30
	releaseStream := make(chan struct{})
	rpc.streamRelease = releaseStream
	root := filepath.Join(t.TempDir(), "dump")
	done := make(chan error, 1)
	go func() {
		_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
			OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
		})
		done <- err
	}()
	waitForLiveBackupRenewal(t, rpc)
	close(releaseStream)
	if err := <-done; err != nil {
		t.Fatalf("RunLiveBackup: %v", err)
	}
	assertLiveBackupEnded(t, rpc, "renewed-token")
}

func TestRunLiveBackupInvalidBeginResponseReleasesPin(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	rpc.begin.ReadTs = 0
	root := filepath.Join(t.TempDir(), "dump")
	_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root, Adapters: AdapterSet{Redis: true}, TTL: time.Minute,
	})
	if err == nil {
		t.Fatal("RunLiveBackup accepted an incomplete BeginBackup response")
	}
	assertLiveBackupEnded(t, rpc, "pin-token")
	assertLiveBackupHasNoManifest(t, root)
}

func TestRunLiveBackupUnavailableScopeLeavesNoManifest(t *testing.T) {
	t.Parallel()
	rpc := successfulLiveBackupRPC()
	root := filepath.Join(t.TempDir(), "dump")
	_, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root,
		Adapters:   AdapterSet{Redis: true},
		Scopes:     []Scope{{Adapter: adapterRedis, Name: "db_1"}},
		TTL:        time.Minute,
	})
	if !errors.Is(err, ErrLiveBackupScope) {
		t.Fatalf("err=%v, want ErrLiveBackupScope", err)
	}
	assertLiveBackupEnded(t, rpc, "pin-token")
	assertLiveBackupHasNoManifest(t, root)
}

func TestCrossAdapterConsistency(t *testing.T) {
	t.Parallel()
	rpc, body := crossAdapterLiveBackupRPC(t)
	root := filepath.Join(t.TempDir(), "dump")
	result, err := RunLiveBackup(context.Background(), rpc, LiveBackupOptions{
		OutputRoot: root,
		Adapters:   AdapterSet{DynamoDB: true, S3: true},
		TTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("RunLiveBackup: %v", err)
	}
	assertCrossAdapterLiveBackup(t, root, body, result)
}

func crossAdapterLiveBackupRPC(t *testing.T) (*fakeLiveBackupRPC, []byte) {
	t.Helper()
	rpc := successfulLiveBackupRPC()
	dynamoSchema := &pb.DynamoTableSchema{
		TableName:            "sessions",
		PrimaryKey:           &pb.DynamoKeySchema{HashKey: "session_id"},
		AttributeDefinitions: map[string]string{"session_id": "S"},
		Generation:           1,
	}
	dynamoItem := &pb.DynamoItem{Attributes: map[string]*pb.DynamoAttributeValue{
		"session_id": sAttr("sess-42"),
		"owner":      sAttr("alice"),
	}}
	const (
		bucket   = "photos"
		object   = "2026/image.jpg"
		uploadID = "upload-1"
	)
	body := []byte("image-at-read-ts-42")
	rpc.listed = []*pb.BackupScope{
		{Adapter: adapterDynamoDB, Scope: "sessions"},
		{Adapter: adapterS3, Scope: bucket},
	}
	rpc.begin.ExpectedKeys = []*pb.BackupExpectedKeys{
		{Adapter: adapterDynamoDB, Scope: "sessions", KeyCount: 2},
		{Adapter: adapterS3, Scope: bucket, KeyCount: 3},
	}
	rpc.records = []*pb.BackupKV{
		{Key: EncodeDDBTableMetaKey("sessions"), Value: encodeSchemaValue(t, dynamoSchema)},
		{Key: EncodeDDBItemKey("sessions", 1, "sess-42", ""), Value: encodeItemValue(t, dynamoItem)},
		{Key: s3keys.BucketMetaKey(bucket), Value: encodeS3BucketMetaValue(t, map[string]any{
			"bucket_name": bucket, "generation": 1,
		})},
		{Key: s3keys.ObjectManifestKey(bucket, 1, object), Value: encodeS3ManifestValue(t, map[string]any{
			"upload_id": uploadID, "size_bytes": len(body), "parts": []map[string]any{
				{"part_no": 1, "size_bytes": len(body), "chunk_count": 1},
			},
		})},
		{Key: s3keys.BlobKey(bucket, 1, object, uploadID, 1, 0), Value: body},
	}
	return rpc, body
}

func assertCrossAdapterLiveBackup(t *testing.T, root string, body []byte, result LiveBackupResult) {
	t.Helper()
	if result.ReadTS != 42 || len(result.Scopes) != 2 {
		t.Fatalf("result = %+v", result)
	}
	item := readItemMap(t, filepath.Join(root, "dynamodb", "sessions", "items", "sess-42.json"))
	if mustSubMap(t, item, "owner")["S"] != "alice" {
		t.Fatalf("DynamoDB item = %v", item)
	}
	gotBody, err := os.ReadFile(filepath.Join(root, "s3", "photos", "2026/image.jpg")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read S3 object: %v", err)
	}
	if string(gotBody) != string(body) {
		t.Fatalf("S3 body = %q, want %q", gotBody, body)
	}
	manifest := readLiveBackupManifest(t, root)
	if manifest.Live == nil || manifest.Live.ReadTS != 42 || manifest.Adapters.DynamoDB == nil || manifest.Adapters.S3 == nil {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func TestExpectedKeysBaselineToleratesTTLExpiry(t *testing.T) {
	t.Parallel()
	scope := Scope{Adapter: adapterRedis, Name: "db_0"}
	selected := map[Scope]struct{}{scope: {}}
	baseline := []*pb.BackupExpectedKeys{{Adapter: scope.Adapter, Scope: scope.Name, KeyCount: 10_000}}
	if err := validateExpectedLiveCounts(selected, map[Scope]uint64{scope: 9_900}, baseline); err != nil {
		t.Fatalf("1%% expiry rejected: %v", err)
	}
	err := validateExpectedLiveCounts(selected, map[Scope]uint64{scope: 9_500}, baseline)
	if !errors.Is(err, ErrCompactionDuringDump) {
		t.Fatalf("5%% shortfall error = %v, want ErrCompactionDuringDump", err)
	}
}

func waitForLiveBackupRenewal(t *testing.T, rpc *fakeLiveBackupRPC) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rpc.mu.Lock()
		renewed := rpc.renewCalls > 0
		rpc.mu.Unlock()
		if renewed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("live backup pin was not renewed")
}

func assertLiveBackupHasNoManifest(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(root, ManifestFilename)); !os.IsNotExist(err) {
		t.Fatalf("manifest exists after failed backup: %v", err)
	}
}

func TestMinimumAcceptedLiveCount(t *testing.T) {
	t.Parallel()
	if got := minimumAcceptedLiveCount(10_000); got != 9_800 {
		t.Fatalf("minimum=%d, want 9800", got)
	}
	if got := integerSqrt(^uint64(0)); got != uint64(^uint32(0)) {
		t.Fatalf("sqrt(max uint64)=%d, want %d", got, uint64(^uint32(0)))
	}
}
