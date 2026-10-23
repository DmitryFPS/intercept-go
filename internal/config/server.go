package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"intercept/internal/domain"
)

// ServerSchemaV1 обозначает текущий формат внутренних настроек сервера.
const ServerSchemaV1 = "intercept-server-config/v1"

// Server содержит секретные настройки транспорта и внешних направлений.
type Server struct {
	Schema                 string                       `json:"schema"`
	UpdatedAt              time.Time                    `json:"updated_at"`
	Listen                 string                       `json:"listen"`
	DeliveryTimeoutMinutes int                          `json:"delivery_timeout_minutes,omitempty"`
	Transport              ServerTransport              `json:"transport"`
	Destinations           map[string]DestinationConfig `json:"destinations"`
}

// ServerTransport описывает закрытую группу, через которую приходят точки.
type ServerTransport struct {
	BotToken    string `json:"bot_token"`
	BotID       int64  `json:"bot_id,omitempty"`
	BotUsername string `json:"bot_username"`
	ChatID      int64  `json:"chat_id"`
	InviteLink  string `json:"invite_link,omitempty"`
}

// DestinationConfig содержит технические параметры одного получателя. Решение
// о доставке находится в отдельно версионируемой матрице маршрутов.
type DestinationConfig struct {
	Kind            domain.DestinationKind `json:"kind"`
	BotToken        string                 `json:"bot_token,omitempty"`
	ChatID          int64                  `json:"chat_id,omitempty"`
	MessageTemplate string                 `json:"message_template,omitempty"`
	Template        string                 `json:"template,omitempty"`
	Folder          string                 `json:"folder,omitempty"`
	Network         string                 `json:"network,omitempty"`
	Protection      string                 `json:"protection,omitempty"`
	Prefix          string                 `json:"prefix,omitempty"`
	Username        string                 `json:"username,omitempty"`
	Password        string                 `json:"password,omitempty"`
}

// Validate проверяет транспорт и все направления до запуска сервера.
func (cfg Server) Validate() error {
	if cfg.Schema != ServerSchemaV1 {
		return fmt.Errorf("неподдерживаемая схема сервера %q", cfg.Schema)
	}
	if strings.TrimSpace(cfg.Listen) == "" {
		return errors.New("не задан локальный адрес интерфейса")
	}
	if cfg.DeliveryTimeoutMinutes != 0 && (cfg.DeliveryTimeoutMinutes < 1 || cfg.DeliveryTimeoutMinutes > 60) {
		return errors.New("срок доставки сервером должен быть от 1 до 60 минут")
	}
	if !validBotToken(cfg.Transport.BotToken) || cfg.Transport.BotID <= 0 || cfg.Transport.ChatID == 0 {
		return errors.New("транспортный бот или группа настроены неверно")
	}
	if strings.TrimSpace(cfg.Transport.BotUsername) == "" {
		return errors.New("не задано имя серверного бота")
	}
	for id, destination := range cfg.Destinations {
		if strings.TrimSpace(id) == "" {
			return errors.New("у направления нет ID")
		}
		switch destination.Kind {
		case domain.DestinationTelegram:
			if !validBotToken(destination.BotToken) || destination.ChatID == 0 {
				return fmt.Errorf("telegram-направление %q заполнено неверно", id)
			}
		case domain.DestinationWord:
			if strings.TrimSpace(destination.Template) == "" || strings.TrimSpace(destination.Folder) == "" {
				return fmt.Errorf("word-направление %q заполнено неверно", id)
			}
		default:
			return fmt.Errorf("направление %q имеет неизвестный тип", id)
		}
	}
	return nil
}

// DeliveryTimeout гарантирует центру не менее тридцати минут на внешние
// доставки. Старые настройки содержали 10 минут, которых недостаточно для
// штатного всплеска от трёх точек при ограничениях Telegram.
func (cfg Server) DeliveryTimeout() time.Duration {
	minutes := cfg.DeliveryTimeoutMinutes
	if minutes < 30 {
		minutes = 30
	}
	return time.Duration(minutes) * time.Minute
}

// DecodeServer читает серверные настройки и запрещает неизвестные поля.
func DecodeServer(reader io.Reader) (Server, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, 2*1024*1024))
	if err != nil {
		return Server{}, fmt.Errorf("прочитать настройки сервера: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cfg Server
	if err := decoder.Decode(&cfg); err != nil {
		return Server{}, fmt.Errorf("разобрать настройки сервера: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Server{}, errors.New("после настроек сервера найдены лишние данные")
	}
	if err := cfg.Validate(); err != nil {
		return Server{}, err
	}
	return cfg, nil
}
