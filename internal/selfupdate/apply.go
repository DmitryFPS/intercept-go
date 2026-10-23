package selfupdate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"intercept/internal/infrastructure/secrets"
)

// ApplyOptions are supplied only by a running trusted application to the
// separately copied helper. The signed package is verified again here.
type ApplyOptions struct {
	BaseDir, Package, Product, FromVersion, ToVersion string
	Application, Address, TokenFile, WorkRoot         string
	OldPID                                            int
	PublicKey                                         string
}

// Apply performs a transactional switch: stop, backup, replace, health-check,
// and either commit or restore both executable files and persistent data.
func Apply(options ApplyOptions) error {
	if options.OldPID <= 0 || options.BaseDir == "" || options.Package == "" || options.WorkRoot == "" {
		return errors.New("неполные параметры обновления")
	}
	// Помощник запускается из WorkRoot, поэтому Windows не позволит удалить сам
	// исполняемый файл до завершения процесса. При любом исходе немедленно
	// удаляем остальное: пакет, распакованную копию и резервную копию данных.
	// В каталоге остаётся только несекретный помощник. Если откат сам не смог
	// восстановить файлы, резервная копия намеренно сохраняется для ремонта.
	preserveBackup := false
	defer func() { cleanupUpdateArtifacts(options, preserveBackup) }()
	verifiedRoot := filepath.Join(options.WorkRoot, "verified")
	plan, err := VerifyAndExtract(options.Package, verifiedRoot, options.Product, options.FromVersion, options.PublicKey)
	if err != nil {
		return finishApply(options, Result{State: "failed", FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: "Повторная проверка пакета не пройдена: " + err.Error(), UpdatedAt: time.Now()}, err)
	}
	if plan.Manifest.Version != options.ToVersion {
		return errors.New("версия пакета изменилась после подготовки")
	}
	if err := waitForProcessExit(options.OldPID, 30*time.Second); err != nil {
		return finishApply(options, Result{State: "failed", FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: "Старая версия не завершилась вовремя; файлы не изменялись.", UpdatedAt: time.Now()}, err)
	}
	backupRoot := filepath.Join(options.WorkRoot, "backup")
	if err := backupInstallation(options.BaseDir, backupRoot, plan.Manifest.Files); err != nil {
		_ = restartApplication(options, options.FromVersion)
		return finishApply(options, Result{State: "failed", FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: "Не удалось создать резервную копию. Старая версия запущена снова.", UpdatedAt: time.Now()}, err)
	}
	if err := installFiles(options.BaseDir, plan.Root, plan.Manifest.Files); err != nil {
		rollbackErr := rollbackInstallation(options.BaseDir, backupRoot, plan.Manifest.Files)
		_ = restartApplication(options, options.FromVersion)
		message := "Установка не завершена; предыдущая версия восстановлена."
		if rollbackErr != nil {
			preserveBackup = true
			message = "Установка и автоматический откат завершились ошибкой. Резервная копия сохранена в " + backupRoot + ". Передайте диагностику администратору."
		}
		return finishApply(options, Result{State: "rolled_back", FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: message, UpdatedAt: time.Now()}, errors.Join(err, rollbackErr))
	}
	process, healthErr := startAndCheck(options, options.ToVersion)
	if healthErr == nil {
		cleanupOldFiles(options.BaseDir, plan.Manifest.Files)
		return finishApply(options, Result{State: "success", FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: "Обновление успешно установлено и проверено.", UpdatedAt: time.Now()}, nil)
	}
	if process != nil {
		_ = process.Kill()
		_, _ = process.Wait()
	}
	rollbackErr := rollbackInstallation(options.BaseDir, backupRoot, plan.Manifest.Files)
	_, restartErr := startAndCheck(options, options.FromVersion)
	message := "Новая версия не прошла проверку. Предыдущая версия автоматически восстановлена и запущена."
	state := "rolled_back"
	if rollbackErr != nil || restartErr != nil {
		state = "failed"
		message = "Новая версия не запустилась, а автоматический откат требует вмешательства администратора."
		if rollbackErr != nil {
			preserveBackup = true
			message += " Резервная копия сохранена в " + backupRoot + "."
		}
	}
	return finishApply(options, Result{State: state, FromVersion: options.FromVersion, ToVersion: options.ToVersion, Message: message, UpdatedAt: time.Now()}, errors.Join(healthErr, rollbackErr, restartErr))
}

func cleanupUpdateArtifacts(options ApplyOptions, preserveBackup bool) {
	paths := []string{
		options.Package,
		filepath.Join(options.WorkRoot, "payload"),
		filepath.Join(options.WorkRoot, "verified"),
	}
	if !preserveBackup {
		paths = append(paths, filepath.Join(options.WorkRoot, "backup"))
	}
	for _, path := range paths {
		_ = os.RemoveAll(path)
	}
}

func backupInstallation(baseDir, backupRoot string, files []File) error {
	for _, item := range files {
		source := filepath.Join(baseDir, filepath.FromSlash(item.Path))
		if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := copyFile(source, filepath.Join(backupRoot, "files", filepath.FromSlash(item.Path))); err != nil {
			return err
		}
	}
	data := filepath.Join(baseDir, "data")
	if _, err := os.Stat(data); err == nil {
		return copyTree(data, filepath.Join(backupRoot, "data"))
	}
	return nil
}

func installFiles(baseDir, staging string, files []File) error {
	for _, item := range files {
		target := filepath.Join(baseDir, filepath.FromSlash(item.Path))
		fresh := target + ".update-new"
		old := target + ".update-old"
		_ = os.Remove(fresh)
		_ = os.Remove(old)
		if err := copyFile(filepath.Join(staging, filepath.FromSlash(item.Path)), fresh); err != nil {
			return err
		}
		if _, err := os.Stat(target); err == nil {
			if err := os.Rename(target, old); err != nil {
				return err
			}
		}
		if err := os.Rename(fresh, target); err != nil {
			return err
		}
	}
	return nil
}

func rollbackInstallation(baseDir, backupRoot string, files []File) error {
	var result error
	for _, item := range files {
		target := filepath.Join(baseDir, filepath.FromSlash(item.Path))
		_ = os.Remove(target)
		backup := filepath.Join(backupRoot, "files", filepath.FromSlash(item.Path))
		if _, err := os.Stat(backup); err == nil {
			result = errors.Join(result, copyFile(backup, target))
		}
		_ = os.Remove(target + ".update-new")
		_ = os.Remove(target + ".update-old")
	}
	backupData := filepath.Join(backupRoot, "data")
	if _, err := os.Stat(backupData); err == nil {
		data := filepath.Join(baseDir, "data")
		if removeErr := os.RemoveAll(data); removeErr != nil {
			result = errors.Join(result, removeErr)
		} else {
			result = errors.Join(result, copyTree(backupData, data))
		}
	}
	return result
}

func cleanupOldFiles(baseDir string, files []File) {
	for _, item := range files {
		_ = os.Remove(filepath.Join(baseDir, filepath.FromSlash(item.Path)) + ".update-old")
	}
}

func copyTree(source, target string) error {
	return filepath.Walk(source, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if info.IsDir() {
			return os.MkdirAll(destination, 0o700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("неподдерживаемый файл данных: %s", relative)
		}
		return copyFile(path, destination)
	})
}

func restartApplication(options ApplyOptions, expectedVersion string) error {
	_, err := startAndCheck(options, expectedVersion)
	return err
}

func startAndCheck(options ApplyOptions, expectedVersion string) (*os.Process, error) {
	command := exec.Command(filepath.Join(options.BaseDir, options.Application), "--no-browser")
	command.Dir = options.BaseDir
	setHiddenProcess(command)
	if err := command.Start(); err != nil {
		return nil, err
	}
	process := command.Process
	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if version, err := healthVersion(options); err == nil && version == expectedVersion {
			return process, nil
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return process, fmt.Errorf("новая версия не прошла проверку запуска: %w", lastErr)
}

func healthVersion(options ApplyOptions) (string, error) {
	token, err := secrets.Load(filepath.Join(options.BaseDir, filepath.FromSlash(options.TokenFile)))
	if err != nil {
		return "", err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	request, err := http.NewRequest(http.MethodGet, "http://"+options.Address+"/api/status", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("X-EAVC-Access", strings.TrimSpace(string(token)))
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("локальный интерфейс вернул " + strconv.Itoa(response.StatusCode))
	}
	var status struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(&status); err != nil {
		return "", err
	}
	return status.Version, nil
}

func finishApply(options ApplyOptions, result Result, applyErr error) error {
	_ = writeResult(filepath.Join(options.BaseDir, "logs", "latest-update.json"), result)
	logPath := filepath.Join(options.BaseDir, "logs", "updates.log")
	_ = os.MkdirAll(filepath.Dir(logPath), 0o700)
	line, _ := json.Marshal(map[string]any{"time": time.Now(), "state": result.State, "from": result.FromVersion, "to": result.ToVersion, "message": result.Message, "error": fmt.Sprint(applyErr)})
	if file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
		_, _ = file.Write(append(line, '\n'))
		_ = file.Close()
	}
	return applyErr
}
