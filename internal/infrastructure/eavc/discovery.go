package eavc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const databaseFilename = "main.sqlite"

// Candidate — проверенная база EAVC, которую можно показать оператору.
type Candidate struct {
	Path       string    `json:"path"`
	ModifiedAt time.Time `json:"modified_at"`
}

// QuickRoots возвращает небольшое число мест рядом с переносимым клиентом.
// Такой поиск выполняется при запуске и не обходит весь компьютер.
func QuickRoots(baseDir string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, 5)
	current := filepath.Clean(baseDir)
	for range 5 {
		if current == "." || current == "" || seen[strings.ToLower(current)] {
			break
		}
		seen[strings.ToLower(current)] = true
		result = append(result, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return result
}

// DriveRoots перечисляет доступные локальные диски без платформенных API.
// На Windows проверяются привычные буквы, на других системах — корень.
func DriveRoots() []string {
	if filepath.Separator != '\\' {
		return []string{string(filepath.Separator)}
	}
	result := make([]string, 0, 4)
	for letter := 'C'; letter <= 'Z'; letter++ {
		root := string(letter) + `:\`
		if info, err := os.Stat(root); err == nil && info.IsDir() {
			result = append(result, root)
		}
	}
	return result
}

// Discover находит только файлы с ожидаемым именем и проверяет их схему.
// Ошибки доступа к отдельным каталогам не прерывают поиск по остальным.
func Discover(ctx context.Context, roots []string, recursive bool) ([]Candidate, error) {
	seen := make(map[string]bool)
	var result []Candidate
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "." || root == "" {
			continue
		}
		if recursive {
			if err := walkCandidates(ctx, root, seen, &result); err != nil {
				return nil, err
			}
			continue
		}
		for _, path := range nearbyCandidates(root) {
			appendCandidate(ctx, path, seen, &result)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].ModifiedAt.Equal(result[right].ModifiedAt) {
			return result[left].Path < result[right].Path
		}
		return result[left].ModifiedAt.After(result[right].ModifiedAt)
	})
	return result, nil
}

func nearbyCandidates(root string) []string {
	return []string{
		filepath.Join(root, databaseFilename),
		filepath.Join(root, "EAVC", databaseFilename),
		filepath.Join(root, "eavc", databaseFilename),
	}
}

func walkCandidates(ctx context.Context, root string, seen map[string]bool, result *[]Candidate) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if entry != nil && entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if path != root && skipDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.EqualFold(entry.Name(), databaseFilename) {
			appendCandidate(ctx, path, seen, result)
		}
		return nil
	})
}

func skipDirectory(name string) bool {
	switch strings.ToLower(name) {
	case "$recycle.bin", "system volume information", "windows", "winsxs", ".git", ".gocache", ".gomodcache", "node_modules":
		return true
	default:
		return false
	}
}

func appendCandidate(ctx context.Context, path string, seen map[string]bool, result *[]Candidate) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return
	}
	key := strings.ToLower(filepath.Clean(abs))
	if seen[key] {
		return
	}
	seen[key] = true
	info, err := os.Stat(abs)
	if err != nil || info.IsDir() {
		return
	}
	checkContext, cancel := context.WithTimeout(ctx, 3*time.Second)
	source, err := Open(checkContext, abs)
	cancel()
	if err != nil {
		return
	}
	_ = source.Close()
	*result = append(*result, Candidate{Path: abs, ModifiedAt: info.ModTime()})
}

// ValidatePath принимает путь к файлу либо к корневой папке EAVC.
func ValidatePath(ctx context.Context, value string) (string, error) {
	value = filepath.Clean(strings.TrimSpace(value))
	if value == "." || value == "" {
		return "", errors.New("укажите папку EAVC или файл main.sqlite")
	}
	if info, err := os.Stat(value); err == nil && info.IsDir() {
		value = filepath.Join(value, databaseFilename)
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	source, err := Open(ctx, abs)
	if err != nil {
		return "", err
	}
	_ = source.Close()
	return abs, nil
}
