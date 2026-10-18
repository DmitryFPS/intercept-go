package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const ClientSchemaV1 = "intercept-client-config/v1"

// Client содержит все настройки точки, подготовленные администратором.
type Client struct {
	Schema       string          `json:"schema"`
	GeneratedAt  time.Time       `json:"generated_at"`
	ClientID     string          `json:"client_id"`
	ClientNumber string          `json:"client_number"`
	ClientName   string          `json:"client_name"`
	EAVC         EAVC            `json:"eavc"`
	Transport    ClientTransport `json:"transport"`
	Cryptography ClientCrypto    `json:"cryptography"`
	Polling      ClientPolling   `json:"polling"`
}

type EAVC struct {
	SQLitePath string `json:"sqlite_path"`
}

type ClientTransport struct {
	BotToken          string `json:"bot_token"`
	BotUsername       string `json:"bot_username"`
	ServerBotUsername string `json:"server_bot_username"`
	ChatID            int64  `json:"chat_id"`
	ThreadID          int64  `json:"thread_id"`
	InviteLink        string `json:"invite_link,omitempty"`
}

type ClientCrypto struct {
	KeyID     string `json:"key_id"`
	KeyBase64 string `json:"key_base64"`
}

type ClientPolling struct {
	EAVCSeconds        int  `json:"eavc_seconds"`
	BatchSize          int  `json:"batch_size"`
	CatchUpExisting    bool `json:"catch_up_existing"`
	MaxEventAgeMinutes int  `json:"max_event_age_minutes"`
}

func (cfg Client) Validate() error {
	if cfg.Schema != ClientSchemaV1 {
		return fmt.Errorf("неподдерживаемая схема настроек %q", cfg.Schema)
	}
	if strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientNumber) == "" || strings.TrimSpace(cfg.ClientName) == "" {
		return errors.New("не заданы ID или название точки")
	}
	if strings.TrimSpace(cfg.EAVC.SQLitePath) == "" {
		return errors.New("не задан путь к базе EAVC")
	}
	if !validBotToken(cfg.Transport.BotToken) {
		return errors.New("токен бота точки имеет неверный формат")
	}
	if cfg.Transport.ChatID == 0 || cfg.Transport.ThreadID <= 0 {
		return errors.New("не заданы транспортная группа или тема точки")
	}
	if strings.TrimSpace(cfg.Transport.BotUsername) == "" || strings.TrimSpace(cfg.Transport.ServerBotUsername) == "" {
		return errors.New("не заданы имена транспортных ботов")
	}
	key, err := cfg.Key()
	if err != nil {
		return err
	}
	if len(key) != 32 || strings.TrimSpace(cfg.Cryptography.KeyID) == "" {
		return errors.New("некорректный ключ шифрования точки")
	}
	if cfg.Polling.EAVCSeconds < 1 || cfg.Polling.EAVCSeconds > 300 {
		return errors.New("период чтения EAVC должен быть от 1 до 300 секунд")
	}
	if cfg.Polling.BatchSize < 1 || cfg.Polling.BatchSize > 1000 {
		return errors.New("размер пакета должен быть от 1 до 1000")
	}
	if cfg.Polling.MaxEventAgeMinutes < 1 || cfg.Polling.MaxEventAgeMinutes > 1440 {
		return errors.New("срок доставки события должен быть от 1 до 1440 минут")
	}
	return nil
}

func (cfg Client) Key() ([]byte, error) {
	key, err := base64.RawStdEncoding.DecodeString(cfg.Cryptography.KeyBase64)
	if err != nil {
		return nil, errors.New("ключ шифрования имеет неверный формат")
	}
	return key, nil
}

// ReadClientFile загружает ограниченный по размеру JSON и запрещает лишние поля.
func ReadClientFile(path string) (Client, error) {
	file, err := os.Open(path)
	if err != nil {
		return Client{}, fmt.Errorf("открыть файл настроек: %w", err)
	}
	defer file.Close()
	return DecodeClient(io.LimitReader(file, 1024*1024))
}

func DecodeClient(reader io.Reader) (Client, error) {
	payload, err := io.ReadAll(reader)
	if err != nil {
		return Client{}, fmt.Errorf("прочитать настройки: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cfg Client
	if err := decoder.Decode(&cfg); err != nil {
		return Client{}, fmt.Errorf("разобрать настройки: %w", err)
	}
	if cfg.Polling.MaxEventAgeMinutes == 0 {
		cfg.Polling.MaxEventAgeMinutes = 30
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Client{}, errors.New("после настроек найдены лишние данные")
	}
	if err := cfg.Validate(); err != nil {
		return Client{}, err
	}
	return cfg, nil
}

func validBotToken(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 2 || len(parts[0]) < 6 || len(parts[1]) < 20 {
		return false
	}
	for _, char := range parts[0] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
