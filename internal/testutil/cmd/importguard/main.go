// Command importguard checks imports in repository production source, including
// files excluded by the current platform or build tags.
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	if err := checkTree("."); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("read production source: %w", walkErr)
		}
		if entry.IsDir() {
			if path != root && ignoredDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		return checkFile(path)
	})
}

func ignoredDirectory(name string) bool {
	return strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "testutil" || strings.HasSuffix(name, "test")
}

func checkFile(path string) error {
	fset := token.NewFileSet()
	source, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		return fmt.Errorf("parse production imports: %w", err)
	}
	for _, spec := range source.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return fmt.Errorf("parse import path: %w", err)
		}
		if forbiddenImport(importPath) {
			return fmt.Errorf("%s: production import of test support %q", fset.Position(spec.Pos()), importPath)
		}
	}
	return nil
}

func forbiddenImport(path string) bool {
	if path == "internal/synctest" || path == "github.com/cockroachdb/pebble/v2/internal/blobtest" {
		return false
	}
	if path == "testing" || strings.HasPrefix(path, "testing/") ||
		path == "github.com/stretchr/testify" || strings.HasPrefix(path, "github.com/stretchr/testify/") ||
		strings.Contains("/"+path+"/", "/internal/testutil/") {
		return true
	}
	for _, part := range strings.Split(path, "/") {
		if strings.HasSuffix(part, "test") {
			return true
		}
	}
	return false
}
