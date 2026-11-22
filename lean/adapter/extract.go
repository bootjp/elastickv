// Command extract translates the side-effect-free retry predicates into Lean.
// An unknown statement, predicate, or argument makes the proof build fail.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
)

const redisRetryFile = "redis_retry.go"

var predicates = []struct{ file, name string }{
	{"route_write_fence.go", "isRouteWriteFencedError"},
	{"redis.go", "isRedisIndeterminate"},
	{redisRetryFile, "isReusableRedisTxnErr"},
	{redisRetryFile, "isRedisComposedRouteErr"},
	{redisRetryFile, "isRetryableRedisTxnErr"},
	{"redis_txn.go", "isAmbiguousRedisExecDispatchErr"},
	{"dynamodb_transact.go", "isRetryableTransactWriteError"},
	{"s3.go", "isRetryableS3MutationErr"},
}

var observed = map[string]string{
	"true":  "true",
	"false": "false",
	"errors.Is(err, kv.ErrTransactionOutcomeUnknown)": "s.unknown",
	"isIndeterminateMutationDispatchFailure(err)":     "s.dispatchLost",
	"errors.Is(err, store.ErrWriteConflict)":          "s.conflict",
	"errors.Is(err, kv.ErrTxnLocked)":                 "s.locked",
	"errors.Is(err, kv.ErrRouteWriteFenced)":          "s.fenced",
	"errors.Is(err, kv.ErrComposed1Violation)":        "s.violation",
	"errors.Is(err, kv.ErrComposed1VersionGCd)":       "s.versionGCd",
}

func source(node ast.Node) string {
	var buf bytes.Buffer
	if err := format.Node(&buf, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return buf.String()
}

func expression(x ast.Expr) string {
	switch e := x.(type) {
	case *ast.ParenExpr:
		return expression(e.X)
	case *ast.UnaryExpr:
		if e.Op == token.NOT {
			return "(!" + expression(e.X) + ")"
		}
	case *ast.BinaryExpr:
		if e.Op == token.LAND || e.Op == token.LOR {
			return "(" + expression(e.X) + " " + e.Op.String() + " " + expression(e.Y) + ")"
		}
	case *ast.CallExpr, *ast.Ident:
		if result, ok := observed[source(x)]; ok {
			return result
		}
	}
	panic("unsupported Go expression: " + source(x))
}

// A sequence consists of guarded early returns followed by a final return.
func body(stmts []ast.Stmt) string {
	if len(stmts) == 0 {
		panic("missing return")
	}
	switch stmt := stmts[0].(type) {
	case *ast.ReturnStmt:
		if len(stmts) == 1 && len(stmt.Results) == 1 {
			return expression(stmt.Results[0])
		}
	case *ast.IfStmt:
		if stmt.Init == nil && stmt.Else == nil {
			return "(if " + expression(stmt.Cond) + " then " + body(stmt.Body.List) +
				" else " + body(stmts[1:]) + ")"
		}
	}
	panic("unsupported Go statement: " + source(stmts[0]))
}

func translate(file, name string) string {
	f, err := parser.ParseFile(token.NewFileSet(), "adapter/"+file, nil, parser.SkipObjectResolution)
	if err != nil {
		panic(err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv == nil && fn.Name.Name == name && source(fn.Type) == "func(err error) bool" {
			return body(fn.Body.List)
		}
	}
	panic("missing predicate " + name)
}

func main() {
	for _, p := range predicates {
		rhs := translate(p.file, p.name)
		fmt.Fprintf(os.Stdout, "def %s (s : Signals) : Bool := %s\n", p.name, rhs)
		observed[p.name+"(err)"] = "(" + p.name + " s)"
	}
}
