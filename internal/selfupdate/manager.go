package selfupdate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Configuration contains product-specific paths while keeping the update
// protocol identical for the station and the center.
type Configuration struct {
	BaseDir       string
	Product       string
	Version       string
	PublicKey     string
	Application   string
	Helper        string
	HealthAddress string
	TokenFile     string
	Logger        *slog.Logger
}

// Result is safe to show to an operator and include in diagnostics.
type Result struct {
	State       string    `json:"state"`
	FromVersion string    `json:"from_version,omitempty"`
	ToVersion   string    `json:"to_version,omitempty"`
	Message     string    `json:"message"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Manager verifies the complete package while the old application is still
// serving work, then delegates only the short switch-over to a temporary helper.
var ErrUpdateInProgress = errors.New("обновление уже выполняется")

type Manager struct {
	config   Configuration
	mu       sync.Mutex
	updating bool
}

func NewManager(configuration Configuration) *Manager {
	cleanupOrphanedWorkRoots(os.TempDir(), time.Now())
	return &Manager{config: configuration}
}

// cleanupOrphanedWorkRoots removes only old completed helper directories.
// A directory containing backup is an unsuccessful rollback and is preserved
// for administrator recovery.
func cleanupOrphanedWorkRoots(tempRoot string, now time.Time) {
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "EAVC-Connect-Update-") {
			continue
		}
		path := filepath.Join(tempRoot, entry.Name())
		if _, err := os.Stat(filepath.Join(path, "backup")); err == nil {
			continue
		}
		info, err := entry.Info()
		if err != nil || now.Sub(info.ModTime()) < 24*time.Hour {
			continue
		}
		_ = os.RemoveAll(path)
	}
}

func (manager *Manager) resultPath() string {
	return filepath.Join(manager.config.BaseDir, "logs", "latest-update.json")
}

// LatestResult returns the most recent update outcome without exposing helper
// command lines, paths outside the installation, or signing material.
func (manager *Manager) LatestResult() Result {
	payload, err := os.ReadFile(manager.resultPath())
	if err != nil {
		return Result{State: "none", Message: "Обновления ещё не выполнялись."}
	}
	var result Result
	if json.Unmarshal(payload, &result) != nil {
		return Result{State: "error", Message: "Журнал последнего обновления повреждён."}
	}
	return result
}

// Prepare starts a trusted helper and returns only after the package is fully
// verified and extracted. The caller may then answer HTTP and stop gracefully.
func (manager *Manager) Prepare(packageReader io.Reader, size int64, currentPID int) (Result, error) {
	manager.mu.Lock()
	if manager.updating {
		manager.mu.Unlock()
		return Result{}, ErrUpdateInProgress
	}
	manager.updating = true
	manager.mu.Unlock()
	prepared := false
	defer func() {
		if !prepared {
			manager.mu.Lock()
			manager.updating = false
			manager.mu.Unlock()
		}
	}()
	if size <= 0 || size > MaxPackageSize {
		return Result{}, errors.New("пакет обновления имеет недопустимый размер")
	}
	if manager.config.PublicKey == "" {
		return Result{}, errors.New("эта сборка не настроена для подписанных обновлений")
	}
	workRoot, err := os.MkdirTemp("", "EAVC-Connect-Update-")
	if err != nil {
		return Result{}, fmt.Errorf("создать временную папку: %w", err)
	}
	packagePath := filepath.Join(workRoot, "update.zip")
	file, err := os.OpenFile(packagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, err
	}
	written, copyErr := io.Copy(file, io.LimitReader(packageReader, MaxPackageSize+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || written != size || written > MaxPackageSize {
		_ = os.RemoveAll(workRoot)
		return Result{}, errors.New("не удалось безопасно сохранить пакет обновления")
	}
	staging := filepath.Join(workRoot, "payload")
	plan, err := VerifyAndExtract(packagePath, staging, manager.config.Product, manager.config.Version, manager.config.PublicKey)
	if err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, err
	}
	if _, err := os.Stat(filepath.Join(plan.Root, manager.config.Application)); err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, errors.New("в пакете отсутствует исполняемый файл приложения")
	}
	expandedSize, err := manifestExpandedSize(plan.Manifest.Files)
	if err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, err
	}
	dataSize, err := directorySize(filepath.Join(manager.config.BaseDir, "data"))
	if err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, fmt.Errorf("проверить размер рабочих данных: %w", err)
	}
	const reserve = int64(64 * 1024 * 1024)
	if err := ensureFreeSpace(workRoot, expandedSize+dataSize+reserve); err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, err
	}
	if err := ensureFreeSpace(manager.config.BaseDir, expandedSize+reserve); err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, err
	}
	helperSource := filepath.Join(manager.config.BaseDir, manager.config.Helper)
	helperTarget := filepath.Join(workRoot, "EAVC-Update-Runner.exe")
	if err := copyFile(helperSource, helperTarget); err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, fmt.Errorf("подготовить помощник обновления: %w", err)
	}
	arguments := []string{
		"apply", "--base-dir=" + manager.config.BaseDir, "--package=" + packagePath,
		"--product=" + manager.config.Product, "--from-version=" + manager.config.Version,
		"--to-version=" + plan.Manifest.Version, "--application=" + manager.config.Application,
		"--address=" + manager.config.HealthAddress, "--token-file=" + manager.config.TokenFile,
		"--old-pid=" + strconv.Itoa(currentPID), "--work-root=" + workRoot,
	}
	command := exec.Command(helperTarget, arguments...)
	command.Dir = workRoot
	setHiddenProcess(command)
	if err := command.Start(); err != nil {
		_ = os.RemoveAll(workRoot)
		return Result{}, fmt.Errorf("запустить помощник обновления: %w", err)
	}
	result := Result{State: "switching", FromVersion: manager.config.Version, ToVersion: plan.Manifest.Version, Message: "Пакет проверен. Выполняется безопасное переключение версии.", UpdatedAt: time.Now()}
	_ = writeResult(manager.resultPath(), result)
	if manager.config.Logger != nil {
		manager.config.Logger.Info("обновление проверено и подготовлено", "from", manager.config.Version, "to", plan.Manifest.Version)
	}
	prepared = true
	return result, nil
}

func directorySize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return errors.New("в рабочих данных найден неподдерживаемый файл")
		}
		if info.Size() > MaxExpandedSize || total > MaxExpandedSize-info.Size() {
			return errors.New("рабочие данные слишком велики для безопасного обновления")
		}
		total += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return total, err
}

func writeResult(path string, result Result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func copyFile(source, target string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
