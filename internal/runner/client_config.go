package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"intercept/internal/config"
	"intercept/internal/infrastructure/eavc"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/infrastructure/telegram"
)

// ImportConfig проверяет полученные настройки и только после этого заменяет рабочие.
func (client *Client) ImportConfig(ctx context.Context, payload []byte) error {
	cfg, err := config.DecodeClient(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	issuer, firstImport, err := client.verifyTrustedIssuer(cfg)
	if err != nil {
		return err
	}
	source, err := eavc.Open(ctx, cfg.EAVC.SQLitePath)
	if err != nil {
		return fmt.Errorf("проверка базы EAVC: %w", err)
	}
	source.Close()
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		return err
	}
	me, err := api.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("проверка Telegram-бота: %w", err)
	}
	if !strings.EqualFold(me.Username, strings.TrimPrefix(cfg.Transport.BotUsername, "@")) {
		return errors.New("токен принадлежит другому Telegram-боту")
	}
	if err := os.MkdirAll(filepath.Dir(client.configPath()), 0o700); err != nil {
		return fmt.Errorf("создать папку данных: %w", err)
	}
	canonical, _ := json.Marshal(cfg)
	if err := secrets.Save(client.configPath(), canonical); err != nil {
		return err
	}
	if firstImport {
		if err := secrets.Save(client.trustedIssuerPath(), issuer); err != nil {
			return fmt.Errorf("сохранить доверенный ключ сервера: %w", err)
		}
	}
	if err := client.restartWorker(client.applicationContext()); err != nil {
		return err
	}
	client.Logger.Info("настройки точки импортированы", "client_id", cfg.ClientID, "client_number", cfg.ClientNumber)
	return nil
}

// Diagnostics собирает сведения, которые оператор может передать для поиска ошибки.
func (client *Client) Diagnostics() ([]byte, error) {
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	status, _ := json.MarshalIndent(client.Status(), "", "  ")
	part, _ := archive.Create("status.json")
	_, _ = part.Write(status)
	for _, name := range []string{"client.log", "client.log.1", "client.log.2"} {
		path := filepath.Join(client.BaseDir, "logs", name)
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		part, _ := archive.Create(filepath.Join("logs", name))
		_, _ = io.Copy(part, io.LimitReader(file, 10*1024*1024))
		file.Close()
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (client *Client) configPath() string {
	return filepath.Join(client.BaseDir, "data", "client-config.dat")
}

func (client *Client) trustedIssuerPath() string {
	return filepath.Join(client.BaseDir, "data", "client-trusted-server.dat")
}

func (client *Client) verifyTrustedIssuer(cfg config.Client) ([]byte, bool, error) {
	issuer, err := cfg.IssuerPublicKey()
	if err != nil {
		return nil, false, err
	}
	trusted, err := secrets.Load(client.trustedIssuerPath())
	if err != nil {
		if errors.Is(rootCause(err), os.ErrNotExist) {
			return append([]byte(nil), issuer...), true, nil
		}
		return nil, false, fmt.Errorf("прочитать доверенный ключ сервера: %w", err)
	}
	if !config.SameIssuer(issuer, trusted) {
		return nil, false, errors.New("файл выпущен другим сервером; обратитесь к администратору")
	}
	return append([]byte(nil), issuer...), false, nil
}

func rootCause(err error) error {
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			return err
		}
		err = unwrapped
	}
}
