package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

func protocolFunc(file, name string) *ast.FuncDecl {
	f, err := parser.ParseFile(token.NewFileSet(), "adapter/"+file, nil, parser.SkipObjectResolution)
	if err != nil {
		panic(err)
	}
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	panic("missing function " + name)
}

const unitParam = "unit"

var protocolAtoms = map[string]string{
	"n": "n", unitParam: unitParam, "start": "start", "now": "now", "seconds": "seconds",
	"int64(unit)": unitParam, "math.MaxInt64": "AdapterProtocols.maxDuration",
	"sqsChangeVisibilityMaxSeconds": "43200", "sqsMillisPerSecond": "1000", "0": "0",
}

var protocolOps = map[token.Token]string{
	token.GTR: ">", token.LEQ: "≤", token.EQL: "=", token.SUB: "-",
	token.QUO: "/", token.MUL: "*", token.LAND: "∧", token.LOR: "∨",
}

func protocolExpr(expr ast.Expr) string {
	if value, ok := protocolAtoms[source(expr)]; ok {
		return value
	}
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return protocolExpr(e.X)
	case *ast.BinaryExpr:
		if op, ok := protocolOps[e.Op]; ok {
			return "(" + protocolExpr(e.X) + " " + op + " " + protocolExpr(e.Y) + ")"
		}
	}
	panic("unsupported numeric Go expression " + source(expr))
}

func guardSource(file, name string) string {
	fn := protocolFunc(file, name)
	if len(fn.Body.List) != 1 {
		panic("expected a single return in " + name)
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		panic("expected a single returned expression in " + name)
	}
	return protocolExpr(ret.Results[0])
}

// Read every field of the actual transaction literal, preserving element
// order. The proof rejects extra dispatch plans or changed keys/values.
func transactionSource(file, name string) string {
	var groups []*ast.CompositeLit
	ast.Inspect(protocolFunc(file, name).Body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.CompositeLit); ok && lit.Type != nil && source(lit.Type) == "kv.OperationGroup[kv.OP]" {
			groups = append(groups, lit)
		}
		return true
	})
	if len(groups) != 1 {
		panic("expected exactly one transaction literal in " + name)
	}
	return transactionFields(groups[0], name)
}

func transactionFields(group *ast.CompositeLit, name string) string {
	var fields []string
	for _, e := range group.Elts {
		field, ok := e.(*ast.KeyValueExpr)
		if !ok {
			panic("expected named transaction field in " + name)
		}
		if elems, ok := field.Value.(*ast.CompositeLit); ok && source(field.Key) == "Elems" {
			for _, elem := range elems.Elts {
				var parts []string
				members, ok := elem.(*ast.CompositeLit)
				if !ok {
					panic("expected transaction operation literal in " + name)
				}
				for _, member := range members.Elts {
					parts = append(parts, source(member))
				}
				fields = append(fields, strconv.Quote("Elem "+strings.Join(parts, ", ")))
			}
		} else {
			fields = append(fields, strconv.Quote(source(field)))
		}
	}
	return "[" + strings.Join(fields, ", ") + "]"
}

func protocolDefinitions() {
	fmt.Println("namespace ProtocolGo")
	fmt.Printf("def ttl (n unit : Int) : Prop := %s\n", guardSource("redis_strings.go", "validRedisTTL"))
	fmt.Printf("def budget (start now seconds : Int) : Prop := %s\n", guardSource("sqs_messages.go", "visibilityExtensionFits"))
	fmt.Printf("def luaFields : List String := %s\n", transactionSource("redis_lua_context.go", "commit"))
	fmt.Printf("def beginFields : List String := %s\n", transactionSource("s3.go", "createMultipartUpload"))
	fmt.Printf("def partFields : List String := %s\n", transactionSource("s3_upload_part.go", "commitS3UploadPart"))
	fmt.Printf("def visibilityFields : List String := %s\n", transactionSource("sqs_messages.go", "changeVisibilityWithRetry"))
	fmt.Printf("def abortFields : List String := %s\n", transactionSource("s3.go", "abortMultipartUpload"))
	fmt.Printf("def multipartFields : List String := %s\n", transactionSource("s3_multipart_complete.go", "commitS3MultipartCompletionAttempt"))
}
