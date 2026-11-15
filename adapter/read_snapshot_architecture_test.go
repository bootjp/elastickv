package adapter

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Latest reads must not reconstruct the old sample-before-fence pattern.
// Exceptions are exact functions with separate contracts, never entire files.
// New handlers must use beginFencedReadSnapshot; fixture-only helpers cannot
// compile into a server binary. This guard also rejects new indirect escapes.
func TestReadSnapshotConstructionIsCentralized(t *testing.T) {
	allowed := map[string]map[string]bool{
		"LastCommitTS": {
			"read_snapshot.go:fencedCommittedReadTS": true,
			"ts.go:eventualSnapshotTS":               true,
			// Raw watermark inspection is explicitly not a leader-fenced snapshot.
			"grpc.go:rawLatestCommitTSSingle": true,
			// Migration/write protocol evidence, not latest client read selection.
			"internal.go:IssueMigrationTimestamp":     true,
			"internal.go:forwardReadWatermarkAdvance": true,
			"internal.go:ApplyTargetStagedReadiness":  true,
		},
		"LatestCommitTS": {
			"read_snapshot.go:beginCatalogReadSnapshot":                                true,
			"migration_ownership_refresh_attestation.go:issueMigrationPartitionReadTS": true,
			// Per-key version inspection for OCC, blob identity, or raw RPC metadata;
			// these are not node-local snapshot selection.
			"grpc.go:rawKeyCommitTS":                                 true,
			"redis_txn.go:validateReadSet":                           true,
			"s3_blob_backfill.go:loadS3BlobBackfillRef":              true,
			"s3_blob_read.go:loadS3ChunkRef":                         true,
			"s3_blob_fetch.go:latestChunkBlobTS":                     true,
			"redis_delta_compactor.go:legacyTTLMillisWithCommitTSAt": true,
		},
		"GlobalLastCommitTS": {}, // best-effort RPC fallback must never select a latest snapshot
		"eventualSnapshotTS": {
			"dynamodb_query_scan.go:resolveDynamoReadSnapshot": true,
			// Speculative schema validation only; actual strong reads fence separately.
			"dynamodb_lease.go:loadSpeculativeSchema":      true,
			"dynamodb_lease.go:queryLeaseKey":              true,
			"dynamodb_lease.go:resolveTransactGetItemKeys": true,
		},
		"BeginReadTimestampThrough": {
			"read_snapshot.go:beginFencedReadSnapshot": true,
			"redis_txn.go:beginTargetReadSnapshot":     true,
			// These consume committed seals or explicit migration snapshots, rather
			// than selecting latest reads from local watermarks.
			"dynamodb_locks.go:sealedReadTimestamp":                                    true,
			"migration_ownership_refresh_attestation.go:issueMigrationPartitionReadTS": true,
			"read_snapshot.go:beginCatalogReadSnapshot":                                true,
			"split_job_runner.go:ensureSplitJobCatalogFence":                           true,
			"split_job_working_source.go:ensureSplitWorkingSource":                     true,
			"split_job_working_source.go:commitWorkingSourceFence":                     true,
		},
		"snapshotTS": {}, "globalSnapshotTS": {}, "readTS": {}, "nextTxnReadTS": {}, "txnStartTS": {},
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fs := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fs, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checkSnapshotFactoryFile(t, fs, name, file, allowed)
	}
}

func checkSnapshotFactoryFile(t *testing.T, fs *token.FileSet, name string, file *ast.File, allowed map[string]map[string]bool) {
	t.Helper()
	for _, decl := range file.Decls {
		if general, ok := decl.(*ast.GenDecl); ok && general.Tok == token.TYPE {
			continue
		}
		scope := filepath.Base(name) + ":<package>"
		var root ast.Node = decl
		if fn, ok := decl.(*ast.FuncDecl); ok {
			if fn.Body == nil {
				continue
			}
			scope = filepath.Base(name) + ":" + fn.Name.Name
			root = fn.Body
		}
		ast.Inspect(root, func(node ast.Node) bool {
			callee := snapshotSourceName(node)
			scopes, guarded := allowed[callee]
			if guarded && !scopes[scope] {
				t.Errorf("%s: %s bypasses the fenced snapshot factory in %s", fs.Position(node.Pos()), callee, scope)
			}
			return true
		})
	}
}

func snapshotSourceName(node ast.Node) string {
	switch n := node.(type) {
	case *ast.Ident:
		return snapshotIdentifierName(n)
	case *ast.SelectorExpr:
		// Exported capabilities must not escape through method values either.
		if ast.IsExported(n.Sel.Name) {
			return n.Sel.Name
		}
	case *ast.CallExpr:
		switch fn := n.Fun.(type) {
		case *ast.Ident:
			return fn.Name
		case *ast.SelectorExpr:
			return fn.Sel.Name
		}
	}
	return ""
}

func isReadHelperReference(name string) bool {
	return name == "eventualSnapshotTS" || strings.HasPrefix(name, "request") || strings.HasPrefix(name, "historical") || name == "authorizeHistoricalRead" || name == "manifestVersionGetAt"
}

func TestSnapshotGuardDistinguishesFunctionAliasesFromTimestampData(t *testing.T) {
	policy := map[string]map[string]bool{"LastCommitTS": {}, "eventualSnapshotTS": {}, "readTS": {}}
	for _, tc := range []struct {
		source   string
		rejected bool
	}{
		{"var sample = store.MVCCStore.LastCommitTS", true},
		{"var sample = eventualSnapshotTS", true},
		{"func handler(){sample:=eventualSnapshotTS;sample(nil,st)}", true},
		{"var readTS uint64;func handler(){_=readTS}", false},
		{"func handler(){readTS:=uint64(42);_=readTS}", false},
		{"func handler(){_=manifest.readTS}", false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "fresh.go", "package adapter;"+tc.source, 0)
		if err != nil {
			t.Fatal(err)
		}
		rejected := len(requestReadViolations("fresh.go", file, policy)) > 0
		if rejected != tc.rejected {
			t.Errorf("rejected=%v, want %v: %s", rejected, tc.rejected, tc.source)
		}
	}
}

func snapshotIdentifierName(n *ast.Ident) string {
	if n.Obj != nil && n.Obj.Kind == ast.Fun {
		return n.Name
	}
	// Cross-file helper references have no parser-local object. Ordinary
	// variables and fields named readTS are data, not function escapes.
	if n.Obj == nil && isReadHelperReference(n.Name) {
		return n.Name
	}
	return ""
}
