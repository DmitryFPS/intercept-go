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
	"time"

	"intercept/internal/config"
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

// CheckTransport отправляет служебный пакет. Зелёное подтверждение появится
// только после того, как сервер увидит пакет и ответит клиентскому боту.
func (client *Client) CheckTransport(ctx context.Context) error {
	protected, err := secrets.Load(client.configPath())
	if err != nil {
		return errors.New("сначала загрузите настройки точки")
	}
	cfg, err := config.DecodeClient(bytes.NewReader(protected))
	if err != nil {
		return err
	}
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		return err
	}
	transport := &telegram.ClientTransport{
		API: api, ChatID: cfg.Transport.ChatID, ThreadID: cfg.Transport.ThreadID,
		ServerBotID:       cfg.Transport.ServerBotID,
		ServerBotUsername: cfg.Transport.ServerBotUsername,
	}
	probeID, err := transport.SendProbe(ctx, cfg.ClientID)
	if err != nil {
		return fmt.Errorf("отправить проверку связи: %w", err)
	}
	client.update(func(status *ClientStatus) {
		client.pendingProbeID = probeID
		status.LastTransportCheck = nil
		status.State = "starting"
		status.Title = "Проверяем связь"
		status.Detail = "Тестовый пакет отправлен. Ожидаем ответ сервера."
	})
	go client.reportTransportCheckTimeout(client.applicationContext(), probeID)
	return nil
}

func (client *Client) reportTransportCheckTimeout(ctx context.Context, probeID string) {
	timer := time.NewTimer(35 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	client.update(func(status *ClientStatus) {
		if client.pendingProbeID != probeID || status.LastTransportCheck != nil {
			return
		}
		status.State = "waiting"
		status.Title = "Сервер не подтвердил проверку"
		status.Detail = "Проверьте Bot-to-Bot у обоих ботов, транспортную группу, тему точки и запущен ли сервер."
		status.TelegramOK = false
	})
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
		if errors.Is(err, os.ErrNotExist) {
			return append([]byte(nil), issuer...), true, nil
		}
		return nil, false, fmt.Errorf("прочитать доверенный ключ сервера: %w", err)
	}
	if !config.SameIssuer(issuer, trusted) {
		return nil, false, errors.New("файл выпущен другим сервером; обратитесь к администратору")
	}
	return append([]byte(nil), issuer...), false, nil
}
