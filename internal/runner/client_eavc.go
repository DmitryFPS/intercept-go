package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"intercept/internal/config"
	"intercept/internal/infrastructure/eavc"
	"intercept/internal/infrastructure/secrets"
)

type clientLocalSettings struct {
	EAVCPath string `json:"eavc_path"`
}

// SearchEAVC ищет базы и возвращает только кандидатов с подходящей схемой.
func (client *Client) SearchEAVC(ctx context.Context, extended bool) ([]eavc.Candidate, error) {
	roots := eavc.QuickRoots(client.BaseDir)
	if extended {
		roots = eavc.DriveRoots()
	}
	candidates, err := eavc.Discover(ctx, roots, extended)
	if err != nil {
		return nil, fmt.Errorf("найти EAVC: %w", err)
	}
	if len(candidates) == 1 {
		if err := client.SelectEAVC(ctx, candidates[0].Path); err != nil {
			return nil, err
		}
	}
	return candidates, nil
}

// SelectEAVC сохраняет локальный путь отдельно от подписанного серверного JSON.
func (client *Client) SelectEAVC(ctx context.Context, value string) error {
	path, err := eavc.ValidatePath(ctx, value)
	if err != nil {
		return fmt.Errorf("проверить базу EAVC: %w", err)
	}
	payload, err := json.Marshal(clientLocalSettings{EAVCPath: path})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(client.localSettingsPath()), 0o700); err != nil {
		return fmt.Errorf("создать папку локальных настроек: %w", err)
	}
	if err := secrets.Save(client.localSettingsPath(), payload); err != nil {
		return fmt.Errorf("сохранить путь EAVC: %w", err)
	}
	client.update(func(status *ClientStatus) { status.EAVCPath = path })
	if _, err := os.Stat(client.configPath()); err == nil {
		return client.restartWorker(client.applicationContext())
	}
	return nil
}

func (client *Client) resolveEAVCPath(ctx context.Context, cfg config.Client) (string, error) {
	if path, err := client.savedEAVCPath(ctx); err == nil && path != "" {
		return path, nil
	}
	configured := strings.TrimSpace(cfg.EAVC.SQLitePath)
	if configured != "" && !strings.EqualFold(configured, "auto") {
		path, err := eavc.ValidatePath(ctx, configured)
		if err != nil {
			return "", fmt.Errorf("путь из файла настроек не подходит: %w", err)
		}
		return path, nil
	}
	candidates, err := eavc.Discover(ctx, eavc.QuickRoots(client.BaseDir), false)
	if err != nil {
		return "", err
	}
	if len(candidates) == 1 {
		return candidates[0].Path, nil
	}
	if len(candidates) > 1 {
		return "", errors.New("найдено несколько баз EAVC; выберите рабочую базу в окне программы")
	}
	return "", errors.New("база EAVC не найдена; нажмите «Найти EAVC» или укажите путь вручную")
}

func (client *Client) savedEAVCPath(ctx context.Context) (string, error) {
	payload, err := secrets.Load(client.localSettingsPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	var settings clientLocalSettings
	if err := json.Unmarshal(payload, &settings); err != nil {
		return "", errors.New("локальные настройки EAVC повреждены")
	}
	return eavc.ValidatePath(ctx, settings.EAVCPath)
}

// configuredEAVCPathForUpdate читает и проверяет формат локального
// пути, но не требует доступности внешней базы в момент обновления.
func (client *Client) configuredEAVCPathForUpdate(cfg config.Client) (string, error) {
	payload, err := secrets.Load(client.localSettingsPath())
	if err == nil {
		var settings clientLocalSettings
		if err := json.Unmarshal(payload, &settings); err != nil {
			return "", errors.New("локальные настройки EAVC повреждены")
		}
		path := strings.TrimSpace(settings.EAVCPath)
		if path == "" {
			return "", errors.New("в локальных настройках не указан путь EAVC")
		}
		return path, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	configured := strings.TrimSpace(cfg.EAVC.SQLitePath)
	if strings.EqualFold(configured, "auto") {
		return "", nil
	}
	return configured, nil
}

func (client *Client) localSettingsPath() string {
	return filepath.Join(client.BaseDir, "data", "client-local.dat")
}
