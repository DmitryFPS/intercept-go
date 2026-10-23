// Package selfupdate verifies and applies signed EAVC Connect update packages.
package selfupdate

import (
	"archive/zip"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	ManifestName   = "update-manifest.json"
	SignatureName  = "update-manifest.sig"
	MaxPackageSize = 256 * 1024 * 1024
)

// File describes one immutable payload file covered by the manifest signature.
type File struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// Manifest identifies the product and every file in a signed update.
type Manifest struct {
	Schema  int    `json:"schema"`
	Product string `json:"product"`
	Version string `json:"version"`
	Files   []File `json:"files"`
}

// Plan is a verified, extracted update ready for the short switch-over phase.
type Plan struct {
	Manifest Manifest
	Root     string
}

// CanonicalManifest returns the stable bytes signed by the release process.
func CanonicalManifest(manifest Manifest) ([]byte, error) {
	copyManifest := manifest
	copyManifest.Product = strings.TrimSpace(copyManifest.Product)
	copyManifest.Version = strings.TrimSpace(copyManifest.Version)
	sort.Slice(copyManifest.Files, func(i, j int) bool { return copyManifest.Files[i].Path < copyManifest.Files[j].Path })
	return json.Marshal(copyManifest)
}

// VerifyAndExtract validates the signature, paths, sizes and hashes before
// writing any payload into a staging directory outside the live installation.
func VerifyAndExtract(packagePath, stagingRoot, product, currentVersion, publicKeyText string) (Plan, error) {
	info, err := os.Stat(packagePath)
	if err != nil {
		return Plan{}, fmt.Errorf("открыть пакет обновления: %w", err)
	}
	if info.Size() <= 0 || info.Size() > MaxPackageSize {
		return Plan{}, errors.New("пакет обновления имеет недопустимый размер")
	}
	archive, err := zip.OpenReader(packagePath)
	if err != nil {
		return Plan{}, fmt.Errorf("открыть ZIP обновления: %w", err)
	}
	defer archive.Close()
	entries := make(map[string]*zip.File, len(archive.File))
	for _, entry := range archive.File {
		name, pathErr := safeRelativePath(entry.Name)
		if pathErr != nil {
			return Plan{}, pathErr
		}
		if entry.FileInfo().IsDir() {
			continue
		}
		if entry.Mode()&os.ModeSymlink != 0 {
			return Plan{}, fmt.Errorf("символические ссылки запрещены: %s", name)
		}
		if _, exists := entries[name]; exists {
			return Plan{}, fmt.Errorf("повторяющийся файл в пакете: %s", name)
		}
		entries[name] = entry
	}
	manifestPayload, err := readZipEntry(entries[ManifestName], 1024*1024)
	if err != nil {
		return Plan{}, fmt.Errorf("прочитать манифест: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestPayload, &manifest); err != nil {
		return Plan{}, fmt.Errorf("прочитать манифест: %w", err)
	}
	if manifest.Schema != 1 || manifest.Product != product || strings.TrimSpace(manifest.Version) == "" {
		return Plan{}, errors.New("пакет предназначен для другого продукта или имеет неизвестный формат")
	}
	if currentVersion != "" && compareVersions(manifest.Version, currentVersion) <= 0 {
		return Plan{}, fmt.Errorf("версия %s не новее установленной %s", manifest.Version, currentVersion)
	}
	canonical, err := CanonicalManifest(manifest)
	if err != nil {
		return Plan{}, err
	}
	signature, err := readZipEntry(entries[SignatureName], 4096)
	if err != nil {
		return Plan{}, fmt.Errorf("прочитать подпись обновления: %w", err)
	}
	publicKey, err := base64.StdEncoding.DecodeString(strings.TrimSpace(publicKeyText))
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return Plan{}, errors.New("в установленной программе отсутствует корректный ключ обновлений")
	}
	decodedSignature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signature)))
	if err != nil || !ed25519.Verify(ed25519.PublicKey(publicKey), canonical, decodedSignature) {
		return Plan{}, errors.New("цифровая подпись пакета обновления недействительна")
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > 64 {
		return Plan{}, errors.New("манифест содержит недопустимое число файлов")
	}
	if err := os.MkdirAll(stagingRoot, 0o700); err != nil {
		return Plan{}, fmt.Errorf("создать временную папку обновления: %w", err)
	}
	seen := make(map[string]bool, len(manifest.Files))
	for _, expected := range manifest.Files {
		name, pathErr := safePayloadPath(expected.Path)
		if pathErr != nil {
			return Plan{}, pathErr
		}
		if seen[name] {
			return Plan{}, fmt.Errorf("повторяющийся файл манифеста: %s", name)
		}
		seen[name] = true
		entry := entries["payload/"+name]
		if entry == nil || int64(entry.UncompressedSize64) != expected.Size || expected.Size < 0 || expected.Size > MaxPackageSize {
			return Plan{}, fmt.Errorf("размер файла обновления не совпадает: %s", name)
		}
		target := filepath.Join(stagingRoot, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return Plan{}, err
		}
		if err := extractAndHash(entry, target, expected); err != nil {
			return Plan{}, err
		}
	}
	return Plan{Manifest: manifest, Root: stagingRoot}, nil
}

func extractAndHash(entry *zip.File, target string, expected File) error {
	source, err := entry.Open()
	if err != nil {
		return err
	}
	defer source.Close()
	temporary := target + ".part"
	output, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(output, hash), io.LimitReader(source, expected.Size+1))
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil || written != expected.Size || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), expected.SHA256) {
		_ = os.Remove(temporary)
		return fmt.Errorf("контрольная сумма файла обновления не совпадает: %s", expected.Path)
	}
	if err := os.Rename(temporary, target); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func safeRelativePath(value string) (string, error) {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	clean := filepath.ToSlash(filepath.Clean(value))
	if value == "" || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") || strings.Contains(clean, ":") {
		return "", fmt.Errorf("небезопасный путь в пакете обновления: %q", value)
	}
	return clean, nil
}

func safePayloadPath(value string) (string, error) {
	clean, err := safeRelativePath(value)
	if err != nil {
		return "", err
	}
	first := strings.ToLower(strings.Split(clean, "/")[0])
	if first == "data" || first == "logs" || first == ".update" {
		return "", fmt.Errorf("пакет пытается заменить рабочие данные: %s", clean)
	}
	return clean, nil
}

func readZipEntry(entry *zip.File, limit int64) ([]byte, error) {
	if entry == nil || int64(entry.UncompressedSize64) > limit {
		return nil, errors.New("файл отсутствует или слишком велик")
	}
	reader, err := entry.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(payload)) > limit {
		return nil, errors.New("файл слишком велик")
	}
	return bytes.Clone(payload), nil
}
