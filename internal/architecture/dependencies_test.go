package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestDependencyDirection не даёт инфраструктуре незаметно просочиться в центр
// приложения. Такой тест полезнее диаграммы: он падает в том же коммите, где
// нарушено направление зависимостей.
func TestDependencyDirection(t *testing.T) {
	t.Parallel()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	rules := []struct {
		directory string
		forbidden []string
	}{
		{
			directory: "internal/domain",
			forbidden: []string{"intercept/internal/"},
		},
		{
			directory: "internal/application",
			forbidden: []string{
				"intercept/internal/config",
				"intercept/internal/defaults",
				"intercept/internal/infrastructure",
				"intercept/internal/interfaces",
				"intercept/internal/runner",
			},
		},
		{
			directory: "internal/protocol",
			forbidden: []string{
				"intercept/internal/application",
				"intercept/internal/infrastructure",
				"intercept/internal/interfaces",
				"intercept/internal/runner",
			},
		},
	}
	for _, rule := range rules {
		directory := filepath.Join(repositoryRoot, filepath.FromSlash(rule.directory))
		packages, err := parser.ParseDir(token.NewFileSet(), directory, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("прочитать %s: %v", rule.directory, err)
		}
		for _, pkg := range packages {
			for filename, file := range pkg.Files {
				checkImports(t, repositoryRoot, filename, file, rule.forbidden)
			}
		}
	}
}

func checkImports(t *testing.T, root, filename string, file *ast.File, forbidden []string) {
	t.Helper()
	for _, declaration := range file.Imports {
		importPath, err := strconv.Unquote(declaration.Path.Value)
		if err != nil {
			t.Fatalf("разобрать import в %s: %v", filename, err)
		}
		for _, prefix := range forbidden {
			if strings.HasPrefix(importPath, prefix) {
				relative, _ := filepath.Rel(root, filename)
				t.Errorf("%s зависит от запрещённого слоя %s", filepath.ToSlash(relative), importPath)
			}
		}
	}
}
