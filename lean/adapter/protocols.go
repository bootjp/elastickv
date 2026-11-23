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
	f, err := parser.ParseFile(token.NewFileSet(), "adapter/"+file, nil, 0)
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

// Trace the request at the unique dispatch site. A retry loop or callback
// identifies one attempt; an extra inner loop would dispatch repeatedly.
func transactionSource(file, name, dispatch, retry string) string {
	return transactionFields(dispatchedGroup(protocolFunc(file, name), dispatch, retry), name)
}

func dispatchScope(stack []ast.Node) string {
	var scopes []string
	for i, node := range stack {
		switch n := node.(type) {
		case *ast.RangeStmt:
			scopes = append(scopes, "range "+source(n.X))
		case *ast.FuncLit:
			call, ok := stack[i-1].(*ast.CallExpr)
			if !ok {
				panic("unexpected dispatch closure")
			}
			scopes = append(scopes, source(call.Fun))
		case *ast.ForStmt, *ast.GoStmt, *ast.DeferStmt:
			panic("unexpected repeated or asynchronous dispatch")
		}
	}
	return strings.Join(scopes, "/")
}

func dispatchedGroup(fn *ast.FuncDecl, dispatch, retry string) *ast.CompositeLit {
	request := dispatchArgument(fn, dispatch, retry)
	if id, ok := request.(*ast.Ident); ok {
		request = requestBinding(fn, id)
	}
	addr, ok := request.(*ast.UnaryExpr)
	if !ok || addr.Op != token.AND {
		panic("expected a dispatched request literal in " + fn.Name.Name)
	}
	group, ok := addr.X.(*ast.CompositeLit)
	if !ok || source(group.Type) != "kv.OperationGroup[kv.OP]" {
		panic("unexpected request type in " + fn.Name.Name)
	}
	return group
}

func dispatchArgument(fn *ast.FuncDecl, dispatch, retry string) ast.Expr {
	var request ast.Expr
	ast.PreorderStack(fn.Body, nil, func(node ast.Node, stack []ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok || !isDispatchSelector(sel.Sel.Name) {
			return true
		}
		call, direct := stack[len(stack)-1].(*ast.CallExpr)
		if !direct || call.Fun != sel || source(sel) != dispatch || request != nil || dispatchScope(stack) != retry {
			panic("unexpected dispatch in " + fn.Name.Name)
		}
		request = call.Args[len(call.Args)-1]
		return true
	})
	return request
}

func isDispatchSelector(name string) bool {
	return name != "WithDispatchVoucher" && strings.Contains(strings.ToLower(name), "dispatch")
}

func requestBinding(fn *ast.FuncDecl, id *ast.Ident) ast.Expr {
	// Reject aliases, assignments and field mutations between construction
	// and dispatch. The only supported uses are the declaration and argument.
	if id.Obj == nil {
		panic("unbound dispatch request")
	}
	uses := 0
	for node := range ast.Preorder(fn.Body) {
		if ref, ok := node.(*ast.Ident); ok && ref.Obj == id.Obj {
			uses++
		}
	}
	decl, ok := id.Obj.Decl.(*ast.AssignStmt)
	if !ok || uses != 2 || decl.Tok != token.DEFINE || len(decl.Lhs) != 1 || len(decl.Rhs) != 1 {
		panic("expected one immutable dispatch request in " + fn.Name.Name)
	}
	return decl.Rhs[0]
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
	fmt.Printf("def luaFields : List String := %s\n", transactionSource("redis_lua_context.go", "commit", "kv.DispatchWithReadTimestamp", ""))
	fmt.Printf("def beginFields : List String := %s\n", transactionSource("s3.go", "createMultipartUpload", "s.dispatchS3MetadataMutation", ""))
	fmt.Printf("def partFields : List String := %s\n", transactionSource("s3_upload_part.go", "commitS3UploadPart", "s.dispatchS3MetadataMutation", ""))
	fmt.Printf("def visibilityFields : List String := %s\n", transactionSource("sqs_messages.go", "changeVisibilityWithRetry", "s.dispatchSQSMutation", "range transactRetryMaxAttempts"))
	fmt.Printf("def abortFields : List String := %s\n", transactionSource("s3.go", "abortMultipartUpload", "s.dispatchS3MetadataMutation", "s.retryS3Mutation"))
	fmt.Printf("def multipartFields : List String := %s\n", transactionSource("s3_multipart_complete.go", "commitS3MultipartCompletionAttempt", "s.dispatchS3MetadataMutation", ""))
}
