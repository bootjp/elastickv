// Command importguard checks imports in repository production source, including
// files excluded by the current platform or build tags: production code must
// not import test support, and must respect the one-way component layering in
// layers.go.
package main

import (
	"errors"
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

// checkTree reports every violation in the tree, not only the first, so one
// run lists all the imports that need fixing.
func checkTree(root string) error {
	var violations []error
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
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
		if err := checkFile(root, path); err != nil {
			violations = append(violations, err)
		}
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	return errors.Join(violations...)
}

func ignoredDirectory(name string) bool {
	return strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "testutil" || strings.HasSuffix(name, "test")
}

func checkFile(root, path string) error {
	fset := token.NewFileSet()
	source, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		return fmt.Errorf("parse production imports: %w", err)
	}
	var violations []error
	for _, spec := range source.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return fmt.Errorf("parse import path: %w", err)
		}
		if forbiddenImport(importPath) {
			violations = append(violations, fmt.Errorf("%s: production import of test support %q", fset.Position(spec.Pos()), importPath))
		}
		if err := checkLayering(moduleDir(root, path), importPath); err != nil {
			violations = append(violations, fmt.Errorf("%s: %w", fset.Position(spec.Pos()), err))
		}
	}
	return errors.Join(violations...)
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
