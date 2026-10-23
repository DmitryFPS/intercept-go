package selfupdate

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVerifyAndExtractSignedPackage(t *testing.T) {
	packagePath, publicKey := writeSignedTestPackage(t, "eavc-connect-station", "0.2.0-rc42", map[string][]byte{
		"EAVC Connect Station.exe": []byte("new executable"),
		"assets/app.js":            []byte("new assets"),
	}, nil)
	staging := filepath.Join(t.TempDir(), "staging")

	plan, err := VerifyAndExtract(packagePath, staging, "eavc-connect-station", "0.2.0-rc41", publicKey)
	if err != nil {
		t.Fatalf("verify signed package: %v", err)
	}
	if plan.Manifest.Version != "0.2.0-rc42" {
		t.Fatalf("unexpected version %q", plan.Manifest.Version)
	}
	payload, err := os.ReadFile(filepath.Join(staging, "assets", "app.js"))
	if err != nil || string(payload) != "new assets" {
		t.Fatalf("extracted payload mismatch: %q, %v", payload, err)
	}
}

func TestVerifyAndExtractRejectsTamperedPayload(t *testing.T) {
	packagePath, publicKey := writeSignedTestPackage(t, "eavc-connect-center", "2.0.0", map[string][]byte{
		"EAVC Connect Center.exe": []byte("expected"),
	}, map[string][]byte{
		"EAVC Connect Center.exe": []byte("tampered"),
	})

	_, err := VerifyAndExtract(packagePath, filepath.Join(t.TempDir(), "staging"), "eavc-connect-center", "1.0.0", publicKey)
	if err == nil || !strings.Contains(err.Error(), "контрольная сумма") {
		t.Fatalf("tampered payload must be rejected, got %v", err)
	}
}

func TestVerifyAndExtractRejectsWrongProductAndDowngrade(t *testing.T) {
	packagePath, publicKey := writeSignedTestPackage(t, "eavc-connect-center", "2.0.0", map[string][]byte{
		"EAVC Connect Center.exe": []byte("center"),
	}, nil)

	if _, err := VerifyAndExtract(packagePath, filepath.Join(t.TempDir(), "wrong"), "eavc-connect-station", "1.0.0", publicKey); err == nil {
		t.Fatal("package for another product must be rejected")
	}
	if _, err := VerifyAndExtract(packagePath, filepath.Join(t.TempDir(), "old"), "eavc-connect-center", "2.0.0", publicKey); err == nil {
		t.Fatal("same or older version must be rejected")
	}
}

func TestVerifyAndExtractRejectsProtectedDataPath(t *testing.T) {
	packagePath, publicKey := writeSignedTestPackage(t, "eavc-connect-center", "2.0.0", map[string][]byte{
		"data/server-config.dat": []byte("must not be replaced"),
	}, nil)

	_, err := VerifyAndExtract(packagePath, filepath.Join(t.TempDir(), "staging"), "eavc-connect-center", "1.0.0", publicKey)
	if err == nil || !strings.Contains(err.Error(), "рабочие данные") {
		t.Fatalf("protected data path must be rejected, got %v", err)
	}
}

func TestInstallAndRollbackRestoreFilesAndData(t *testing.T) {
	base := filepath.Join(t.TempDir(), "application")
	stage := filepath.Join(t.TempDir(), "stage")
	backup := filepath.Join(t.TempDir(), "backup")
	mustWriteTestFile(t, filepath.Join(base, "app.exe"), "old app")
	mustWriteTestFile(t, filepath.Join(base, "data", "queue.db"), "old data")
	mustWriteTestFile(t, filepath.Join(stage, "app.exe"), "new app")
	mustWriteTestFile(t, filepath.Join(stage, "new.dll"), "new library")
	files := []File{{Path: "app.exe"}, {Path: "new.dll"}}

	if err := backupInstallation(base, backup, files); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := installFiles(base, stage, files); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustWriteTestFile(t, filepath.Join(base, "data", "queue.db"), "changed data")
	if err := rollbackInstallation(base, backup, files, true); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertTestFile(t, filepath.Join(base, "app.exe"), "old app")
	assertTestFile(t, filepath.Join(base, "data", "queue.db"), "old data")
	if _, err := os.Stat(filepath.Join(base, "new.dll")); !os.IsNotExist(err) {
		t.Fatalf("new file remains after rollback: %v", err)
	}
}

func TestRollbackAfterNormalStartPreservesCurrentQueue(t *testing.T) {
	base := filepath.Join(t.TempDir(), "application")
	backup := filepath.Join(t.TempDir(), "backup")
	mustWriteTestFile(t, filepath.Join(base, "app.exe"), "new app")
	mustWriteTestFile(t, filepath.Join(base, "data", "queue.db"), "new queue state")
	mustWriteTestFile(t, filepath.Join(backup, "files", "app.exe"), "old app")
	mustWriteTestFile(t, filepath.Join(backup, "data", "queue.db"), "old queue state")

	if err := rollbackInstallation(base, backup, []File{{Path: "app.exe"}}, false); err != nil {
		t.Fatal(err)
	}
	assertTestFile(t, filepath.Join(base, "app.exe"), "old app")
	assertTestFile(t, filepath.Join(base, "data", "queue.db"), "new queue state")
}

func TestCleanupUpdateArtifactsRemovesBackupButKeepsHelper(t *testing.T) {
	workRoot := t.TempDir()
	packagePath := filepath.Join(workRoot, "update.zip")
	for path, value := range map[string]string{
		packagePath: "signed package",
		filepath.Join(workRoot, "payload", "app"):         "prepared payload",
		filepath.Join(workRoot, "verified", "app"):        "verified payload",
		filepath.Join(workRoot, "backup", "data"):         "sensitive backup",
		filepath.Join(workRoot, "EAVC-Update-Runner.exe"): "running helper",
	} {
		mustWriteTestFile(t, path, value)
	}

	cleanupUpdateArtifacts(ApplyOptions{Package: packagePath, WorkRoot: workRoot}, false)
	for _, path := range []string{packagePath, filepath.Join(workRoot, "payload"), filepath.Join(workRoot, "verified"), filepath.Join(workRoot, "backup")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("temporary update artifact remains: %s (%v)", path, err)
		}
	}
	assertTestFile(t, filepath.Join(workRoot, "EAVC-Update-Runner.exe"), "running helper")
}

func TestCleanupUpdateArtifactsPreservesFailedRollbackBackup(t *testing.T) {
	workRoot := t.TempDir()
	packagePath := filepath.Join(workRoot, "update.zip")
	mustWriteTestFile(t, packagePath, "signed package")
	mustWriteTestFile(t, filepath.Join(workRoot, "verified", "app"), "verified")
	mustWriteTestFile(t, filepath.Join(workRoot, "backup", "data", "queue.db"), "recovery copy")

	cleanupUpdateArtifacts(ApplyOptions{Package: packagePath, WorkRoot: workRoot}, true)
	if _, err := os.Stat(packagePath); !os.IsNotExist(err) {
		t.Fatalf("package remains after cleanup: %v", err)
	}
	assertTestFile(t, filepath.Join(workRoot, "backup", "data", "queue.db"), "recovery copy")
}

func TestCleanupOrphanedWorkRootsKeepsRecoveryBackup(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-48 * time.Hour)
	completed := filepath.Join(root, "EAVC-Connect-Update-completed")
	recovery := filepath.Join(root, "EAVC-Connect-Update-recovery")
	unrelated := filepath.Join(root, "another-program")
	for _, path := range []string{completed, recovery, unrelated} {
		mustWriteTestFile(t, filepath.Join(path, "helper.exe"), "helper")
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	mustWriteTestFile(t, filepath.Join(recovery, "backup", "data", "queue.db"), "recovery")

	cleanupOrphanedWorkRoots(root, time.Now())
	if _, err := os.Stat(completed); !os.IsNotExist(err) {
		t.Fatalf("completed work root remains: %v", err)
	}
	assertTestFile(t, filepath.Join(recovery, "backup", "data", "queue.db"), "recovery")
	assertTestFile(t, filepath.Join(unrelated, "helper.exe"), "helper")
}

func TestManagerRejectsConcurrentPrepare(t *testing.T) {
	manager := &Manager{updating: true}
	_, err := manager.Prepare(strings.NewReader("x"), 1, 1)
	if !errors.Is(err, ErrUpdateInProgress) {
		t.Fatalf("parallel update must be rejected: %v", err)
	}
}

func TestManifestRejectsExcessiveCumulativeExpandedSize(t *testing.T) {
	_, err := manifestExpandedSize([]File{
		{Path: "one", Size: MaxPackageSize},
		{Path: "two", Size: MaxPackageSize},
		{Path: "three", Size: 1},
	})
	if err == nil || !strings.Contains(err.Error(), "слишком велик") {
		t.Fatalf("чрезмерный распакованный размер должен быть отклонён: %v", err)
	}
}

func writeSignedTestPackage(t *testing.T, product, version string, signedFiles, archiveFiles map[string][]byte) (string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Schema: 1, Product: product, Version: version}
	for name, payload := range signedFiles {
		hash := sha256.Sum256(payload)
		manifest.Files = append(manifest.Files, File{Path: name, Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])})
	}
	canonical, err := CanonicalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPayload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if archiveFiles == nil {
		archiveFiles = signedFiles
	}
	packagePath := filepath.Join(t.TempDir(), "update.zip")
	output, err := os.Create(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	write := func(name string, payload []byte) {
		entry, createErr := writer.Create(name)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, writeErr := entry.Write(payload); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	write(ManifestName, manifestPayload)
	write(SignatureName, []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, canonical))))
	for name, payload := range archiveFiles {
		write("payload/"+filepath.ToSlash(name), payload)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	return packagePath, base64.StdEncoding.EncodeToString(publicKey)
}

func mustWriteTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertTestFile(t *testing.T, path, expected string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != expected {
		t.Fatalf("%s = %q, want %q", path, payload, expected)
	}
}
