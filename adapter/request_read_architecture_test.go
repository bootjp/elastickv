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

// New data reads use the capability-typed port. The legacy At bridge only
// resolves numbers in existing version-aware helpers, never in new handlers.
func TestAdaptersRequireCapabilityReadPorts(t *testing.T) {
	allowed := requestReadPolicy()
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
		for _, violation := range requestReadViolations(name, file, allowed) {
			t.Error(violation)
		}
	}
}

func requestReadPolicy() map[string]map[string]bool {
	allowed := make(map[string]map[string]bool)
	allowed["requestReadSnapshot"] = map[string]bool{"request_read.go:requestGetAt": true, "request_read.go:requestScanAt": true, "request_read.go:manifestVersionGetAt": true, "request_read_ports.go:requestScanPhysicalAt": true}

	for _, name := range []string{"GetAt", "ScanAt", "ExistsAt", "ScanKeysAt", "ReverseScanAt", "GetGroupAt", "ScanGroupAt", "ReverseScanGroupAt", "ScanGroupKeysAt", "GetAtWithReadFence", "ScanAtWithReadFence", "ScanKeysAtWithReadFence", "ScanGroupOwnedAt", "ScanAtPhysicalLimit", "ReverseScanAtPhysicalLimit"} {
		allowed[name] = map[string]bool{}
		allowed["request"+name] = legacyRequestReadScopes
		allowed["requestReadSnapshot"]["request_read_ports.go:request"+name] = true
	}
	allowed["requestScanPhysicalAt"] = legacyRequestReadScopes
	allowed["Historical"] = map[string]bool{
		"request_read.go:requestReadSnapshot":     true,
		"request_read.go:historicalGetAt":         true,
		"request_read.go:historicalScanAt":        true,
		"request_read.go:authorizeHistoricalRead": true,
		"grpc.go:RawScanAt":                       true,
		"grpc.go:resolveRawReadSnapshot":          true,
	}
	allowed["Issued"] = map[string]bool{"request_read.go:requestReadSnapshot": true, "request_read.go:checkIssuedGet": true}
	allowed["historicalGetAt"] = historicalRequestReadScopes
	allowed["manifestVersionGetAt"] = map[string]bool{"s3_blob_read.go:readLegacyS3ObjectChunk": true, "s3_blob_read.go:loadS3ChunkRef": true}
	allowed["historicalScanAt"] = historicalRequestReadScopes
	allowed["authorizeHistoricalRead"] = historicalRequestReadScopes
	return allowed
}

func requestReadViolations(name string, file *ast.File, allowed map[string]map[string]bool) []string {
	var violations []string
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
			call := snapshotSourceName(node)
			scopes, guarded := allowed[call]
			if guarded && !scopes[scope] {
				violations = append(violations, scope+": unapproved raw/version read "+call)
			}
			return true
		})
	}
	return violations
}

func TestCapabilityReadPortGuardRejectsNewRawOrHistoricalEscapes(t *testing.T) {
	for _, body := range []string{
		"store.GetAt(ctx,key,oldTimestamp)",
		"store.ScanAt(ctx,start,end,10,oldTimestamp)",
		"old := requestGetAt; old(ctx, store, key, oldTimestamp)",
		"request.Historical(ctx,oldTimestamp)",
		"request.Issued(ctx,oldTimestamp)",
		"old := manifestVersionGetAt; old(ctx,store,key,oldTimestamp)",
		"old := requestReadSnapshot; old(ctx,store,oldTimestamp)",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "fresh.go", "package adapter; func fresh(){"+body+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(requestReadViolations("fresh.go", file, requestReadPolicy())) == 0 {
			t.Fatalf("guard accepted %s", body)
		}
	}
}

// Public ingress owns a new capability scope even when its caller supplies one.
// Legacy At helpers may only rely on historical semantics outside this boundary.
func TestPublicDataIngressOwnsReadRequests(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{"s3.go:handle": true, "sqs.go:handle": true, "dynamodb.go:handle": true, "grpc.go:Get": true, "grpc.go:Scan": true, "grpc.go:RawGet": true, "grpc.go:RawScanAt": true}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checkReadIngressFile(t, name, file, expected)
	}
	for scope := range expected {
		t.Errorf("required ingress missing: %s", scope)
	}
}

func TestCapabilityReadPortGuardRejectsPackageAliases(t *testing.T) {
	for _, source := range []string{"var read = requestGetAt", "var read = store.MVCCStore.GetAt", "var read = request.Historical"} {
		file, err := parser.ParseFile(token.NewFileSet(), "fresh.go", "package adapter; "+source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(requestReadViolations("fresh.go", file, requestReadPolicy())) == 0 {
			t.Fatalf("guard accepted %s", source)
		}
	}
}

func isAdminDataIngress(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || !strings.HasPrefix(fn.Name.Name, "Admin") || fn.Type.Params == nil {
		return false
	}
	for _, param := range fn.Type.Params.List {
		if sel, ok := param.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Context" {
			return true
		}
	}
	return false
}
func ownsReadRequest(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch snapshotSourceName(call) {
			case "NewReadRequest", "newReadRequest":
				found = true
			}
		}
		return true
	})
	return found
}

func checkReadIngressFile(t *testing.T, name string, file *ast.File, expected map[string]bool) {
	t.Helper()
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		scope := name + ":" + fn.Name.Name
		if !expected[scope] && !isAdminDataIngress(fn) {
			continue
		}
		if !ownsReadRequest(fn.Body) {
			t.Errorf("%s must create a request-owned read scope", scope)
		}
		delete(expected, scope)
	}
}
