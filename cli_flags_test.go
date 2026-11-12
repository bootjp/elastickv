package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCLIFlagsReadOnlyByMain keeps grouped flag values flowing down from
// run(): a helper that read cliFlags directly would bring back the hidden
// global inputs that the per-area flag structs replaced.
func TestCLIFlagsReadOnlyByMain(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{"main.go": true}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if ok && ident.Name == "cliFlags" && !allowed[name] {
				t.Errorf("%s: cliFlags is read outside main(); pass the area's flags down instead", fset.Position(ident.Pos()))
			}
			return true
		})
	}
}

// TestRootPackageHoldsOnlyMainGo keeps the server binary's root package to
// main.go: every other file belongs in a package below it (internal/server
// for the assembly), where the import guard enforces the layering.
func TestRootPackageHoldsOnlyMainGo(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("main*.go")
	require.NoError(t, err)
	require.Equal(t, []string{"main.go"}, files)
}
