package config

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	_ "time/tzdata"
)

// ClientSchemaV2 обозначает текущий формат подписанного файла точки.
const ClientSchemaV2 = "intercept-client-config/v2"

// Client содержит все настройки точки, подготовленные администратором.
type Client struct {
	Schema       string             `json:"schema"`
	GeneratedAt  time.Time          `json:"generated_at"`
	ClientID     string             `json:"client_id"`
	ClientNumber string             `json:"client_number"`
	ClientName   string             `json:"client_name"`
	EAVC         EAVC               `json:"eavc"`
	Transport    ClientTransport    `json:"transport"`
	Cryptography ClientCrypto       `json:"cryptography"`
	Polling      ClientPolling      `json:"polling"`
	Authenticity ClientAuthenticity `json:"authenticity"`
}

// IssuerFingerprint возвращает разбитый на группы SHA-256 отпечаток ключа центра.
func IssuerFingerprint(key ed25519.PublicKey) string {
	digest := sha256.Sum256(key)
	// 64 бита достаточно для ручной сверки, а четыре группы можно без ошибок
	// продиктовать или перенести на рабочую точку.
	hexValue := strings.ToUpper(fmt.Sprintf("%x", digest[:8]))
	groups := make([]string, 0, len(hexValue)/4)
	for index := 0; index < len(hexValue); index += 4 {
		groups = append(groups, hexValue[index:index+4])
	}
	return strings.Join(groups, "-")
}

// NormalizeFingerprint убирает разделители для безопасного сравнения кода.
func NormalizeFingerprint(value string) string {
	return strings.ToUpper(strings.Map(func(char rune) rune {
		if char >= '0' && char <= '9' || char >= 'A' && char <= 'F' || char >= 'a' && char <= 'f' {
			return char
		}
		return -1
	}, value))
}

// EAVC описывает источник данных, не смешивая его путь с настройками Telegram.
type EAVC struct {
	SQLitePath string `json:"sqlite_path"`
	TimeZone   string `json:"time_zone"`
}

// ClientTransport содержит адрес транспортной группы и личного бота точки.
type ClientTransport struct {
	BotToken          string `json:"bot_token"`
	BotUsername       string `json:"bot_username"`
	ServerBotID       int64  `json:"server_bot_id,omitempty"`
	ServerBotUsername string `json:"server_bot_username"`
	ChatID            int64  `json:"chat_id"`
	ThreadID          int64  `json:"thread_id"`
	InviteLink        string `json:"invite_link,omitempty"`
}

// ClientCrypto содержит индивидуальный ключ шифрования одной точки.
type ClientCrypto struct {
	KeyID     string `json:"key_id"`
	KeyBase64 string `json:"key_base64"`
}

// ClientPolling задаёт интервалы чтения, размер порции и срок события.
type ClientPolling struct {
	EAVCSeconds        int  `json:"eavc_seconds"`
	BatchSize          int  `json:"batch_size"`
	CatchUpExisting    bool `json:"catch_up_existing"`
	MaxEventAgeMinutes int  `json:"max_event_age_minutes"`
}

// ClientAuthenticity подтверждает, что файл подготовлен доверенным сервером.
type ClientAuthenticity struct {
	Algorithm       string `json:"algorithm"`
	IssuerPublicKey string `json:"issuer_public_key"`
	Signature       string `json:"signature"`
}

// Validate проверяет полноту настроек до их сохранения или запуска клиента.
func (cfg Client) Validate() error {
	if cfg.Schema != ClientSchemaV2 {
		return fmt.Errorf("неподдерживаемая схема настроек %q", cfg.Schema)
	}
	if strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientNumber) == "" || strings.TrimSpace(cfg.ClientName) == "" {
		return errors.New("не заданы ID или название точки")
	}
	if strings.TrimSpace(cfg.EAVC.SQLitePath) == "" {
		return errors.New("не задан путь к базе EAVC")
	}
	if _, err := time.LoadLocation(cfg.EAVCTimeZone()); err != nil {
		return errors.New("неверно указан часовой пояс EAVC")
	}
	if !validBotToken(cfg.Transport.BotToken) {
		return errors.New("токен бота точки имеет неверный формат")
	}
	if cfg.Transport.ChatID == 0 || cfg.Transport.ThreadID <= 0 {
		return errors.New("не заданы транспортная группа или тема точки")
	}
	if cfg.Transport.ServerBotID <= 0 || strings.TrimSpace(cfg.Transport.BotUsername) == "" ||
		strings.TrimSpace(cfg.Transport.ServerBotUsername) == "" {
		return errors.New("не заданы идентификаторы транспортных ботов")
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
	if cfg.Authenticity.Algorithm != "Ed25519" {
		return errors.New("неподдерживаемая подпись файла настроек")
	}
	if _, err := cfg.IssuerPublicKey(); err != nil {
		return err
	}
	if _, err := cfg.signature(); err != nil {
		return err
	}
	return nil
}

// EAVCTimeZone сохраняет совместимость с уже выданными конфигурациями v2.
func (cfg Client) EAVCTimeZone() string {
	if value := strings.TrimSpace(cfg.EAVC.TimeZone); value != "" {
		return value
	}
	return "Europe/Moscow"
}

// IssuerPublicKey возвращает проверенный открытый ключ сервера-издателя.
func (cfg Client) IssuerPublicKey() (ed25519.PublicKey, error) {
	key, err := base64.RawStdEncoding.DecodeString(cfg.Authenticity.IssuerPublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("открытый ключ сервера имеет неверный формат")
	}
	return ed25519.PublicKey(key), nil
}

func (cfg Client) signature() ([]byte, error) {
	signature, err := base64.RawStdEncoding.DecodeString(cfg.Authenticity.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return nil, errors.New("подпись файла настроек имеет неверный формат")
	}
	return signature, nil
}

// SignClient подписывает все поля конфигурации ключом конкретного сервера.
func SignClient(cfg Client, privateKey ed25519.PrivateKey) (Client, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Client{}, errors.New("закрытый ключ подписи сервера повреждён")
	}
	cfg.Authenticity = ClientAuthenticity{
		Algorithm:       "Ed25519",
		IssuerPublicKey: base64.RawStdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey)),
	}
	payload, err := clientSigningPayload(cfg)
	if err != nil {
		return Client{}, err
	}
	cfg.Authenticity.Signature = base64.RawStdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if err := cfg.Validate(); err != nil {
		return Client{}, err
	}
	return cfg, nil
}

// VerifyClient отвергает подменённые или повреждённые поля конфигурации.
func VerifyClient(cfg Client) error {
	publicKey, err := cfg.IssuerPublicKey()
	if err != nil {
		return err
	}
	signature, err := cfg.signature()
	if err != nil {
		return err
	}
	payload, err := clientSigningPayload(cfg)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("подпись файла настроек недействительна")
	}
	return nil
}

func clientSigningPayload(cfg Client) ([]byte, error) {
	cfg.Authenticity.Signature = ""
	payload, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("подготовить данные для подписи: %w", err)
	}
	return payload, nil
}

// SameIssuer сравнивает открытые ключи без утечки по времени выполнения.
func SameIssuer(left, right ed25519.PublicKey) bool {
	return len(left) == len(right) && subtle.ConstantTimeCompare(left, right) == 1
}

// Key декодирует индивидуальный ключ шифрования транспортных пакетов точки.
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

// DecodeClient читает, проверяет и подтверждает подпись клиентского JSON.
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
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Client{}, errors.New("после настроек найдены лишние данные")
	}
	if err := cfg.Validate(); err != nil {
		return Client{}, err
	}
	if err := VerifyClient(cfg); err != nil {
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
