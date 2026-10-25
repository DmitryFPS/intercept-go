package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"intercept/internal/config"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/infrastructure/telegram"
	"intercept/internal/provisioning"
)

const provisionedClientActivationName = "EAVC Connect Station.activation"

// ImportConfig проверяет полученные настройки и только после этого заменяет рабочие.
// При первой установке доверие задаёт сам комплект, полученный от администратора.
// Все последующие файлы обязаны быть подписаны тем же центром.
func (client *Client) ImportConfig(ctx context.Context, payload []byte) error {
	cfg, err := config.DecodeClient(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if current, loadErr := secrets.Load(client.configPath()); loadErr == nil {
		existing, decodeErr := config.DecodeClient(bytes.NewReader(current))
		if decodeErr != nil {
			return fmt.Errorf("прочитать установленные настройки: %w", decodeErr)
		}
		if existing.ClientID != cfg.ClientID {
			return errors.New("файл относится к другой рабочей точке; сначала сохраните диагностику и переустановите программу для нужной точки")
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return loadErr
	}
	if _, err := secrets.Load(client.trustedIssuerPath()); errors.Is(err, os.ErrNotExist) {
		return errors.New("первичная настройка принимается только из защищённого файла активации с отдельным кодом")
	} else if err != nil {
		return fmt.Errorf("прочитать доверенный ключ центра: %w", err)
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
	canonical, _ := json.Marshal(cfg)
	if err := client.persistConfig(canonical, issuer, firstImport); err != nil {
		return err
	}
	if err := client.restartWorker(client.applicationContext()); err != nil {
		return err
	}
	client.Logger.Info("настройки точки импортированы", "client_id", cfg.ClientID, "client_number", cfg.ClientNumber)
	return nil
}

// ActivateProvisionedConfig принимает отдельно переданный код и запускает
// клиент только после успешной аутентифицированной расшифровки комплекта.
func (client *Client) ActivateProvisionedConfig(code string) error {
	if err := client.installProvisionedActivation(code); err != nil {
		return err
	}
	return client.restartWorker(client.applicationContext())
}

func (client *Client) installProvisionedActivation(code string) error {
	if err := provisioning.ValidateCode(code); err != nil {
		return err
	}
	path := filepath.Join(client.BaseDir, provisionedClientActivationName)
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("файл активации не найден рядом с программой")
	}
	if err != nil {
		return fmt.Errorf("прочитать файл активации: %w", err)
	}
	plain, err := provisioning.Open(payload, code)
	if err != nil {
		return err
	}
	cfg, err := config.DecodeClient(bytes.NewReader(plain))
	if err != nil {
		return fmt.Errorf("проверить подготовленные настройки: %w", err)
	}
	if err := client.validateInstalledClientIdentity(cfg); err != nil {
		return err
	}
	issuer, firstImport, err := client.verifyTrustedIssuer(cfg)
	if err != nil {
		return err
	}
	canonical, _ := json.Marshal(cfg)
	if err := client.persistConfig(canonical, issuer, firstImport); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		client.Logger.Warn("не удалось удалить использованный файл активации", "error", err)
	}
	client.Logger.Info("точка безопасно активирована", "client_id", cfg.ClientID, "client_number", cfg.ClientNumber)
	return nil
}

func (client *Client) validateInstalledClientIdentity(next config.Client) error {
	current, err := secrets.Load(client.configPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	existing, err := config.DecodeClient(bytes.NewReader(current))
	if err != nil {
		return fmt.Errorf("прочитать установленные настройки: %w", err)
	}
	if existing.ClientID != next.ClientID {
		return errors.New("комплект относится к другой рабочей точке; для смены точки нужна чистая переустановка")
	}
	return nil
}

func (client *Client) persistConfig(payload, issuer []byte, firstImport bool) error {
	if err := os.MkdirAll(filepath.Dir(client.configPath()), 0o700); err != nil {
		return fmt.Errorf("создать папку данных: %w", err)
	}
	if firstImport {
		if err := secrets.Save(client.trustedIssuerPath(), issuer); err != nil {
			return fmt.Errorf("сохранить доверенный ключ центра: %w", err)
		}
	}
	if err := secrets.Save(client.configPath(), payload); err != nil {
		return fmt.Errorf("сохранить настройки точки: %w", err)
	}
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
	key, err := cfg.Key()
	if err != nil {
		return err
	}
	transport.Key = key
	probeID, err := transport.SendProbe(ctx, cfg.ClientID)
	if err != nil {
		return fmt.Errorf("отправить проверку связи: %w", err)
	}
	client.update(func(status *ClientStatus) {
		client.pendingProbeID = probeID
		status.LastTransportCheck = nil
		status.State = "starting"
		status.Title = "Проверяем связь"
		status.Detail = "Контрольный пакет отправлен. Ожидаем ответ центра."
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
	status, _ := json.MarshalIndent(client.OperationalStatus(), "", "  ")
	part, _ := archive.Create("status.json")
	_, _ = part.Write(status)
	if err := addDiagnosticLogs(archive, client.BaseDir, "client.log"); err != nil {
		_ = archive.Close()
		return nil, err
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
