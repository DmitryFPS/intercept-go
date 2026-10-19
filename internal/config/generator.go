package config

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"time"

	"intercept/internal/protocol"
)

// ClientRequest — данные мастера, которые администратор проверил до экспорта.
type ClientRequest struct {
	Number            string
	Name              string
	EAVCPath          string
	TimeZone          string
	BotToken          string
	BotUsername       string
	ServerBotUsername string
	InviteLink        string
	ChatID            int64
	ThreadID          int64
	CatchUpExisting   bool
}

// GenerateClient создаёт готовые настройки и не требует действий оператора.
func GenerateClient(request ClientRequest, now time.Time, random io.Reader) (Client, error) {
	if random == nil {
		random = rand.Reader
	}
	if strings.TrimSpace(request.Number) == "" || strings.TrimSpace(request.Name) == "" {
		return Client{}, errors.New("не заданы номер или название точки")
	}
	clientID, err := protocol.UUIDv7(now, random)
	if err != nil {
		return Client{}, err
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(random, key); err != nil {
		return Client{}, err
	}
	keyID, err := protocol.UUIDv7(now.Add(time.Nanosecond), random)
	if err != nil {
		return Client{}, err
	}
	eavcPath := strings.TrimSpace(request.EAVCPath)
	if eavcPath == "" {
		eavcPath = "auto"
	}
	timeZone := strings.TrimSpace(request.TimeZone)
	if timeZone == "" {
		timeZone = "Europe/Moscow"
	}
	cfg := Client{
		Schema: ClientSchemaV2, GeneratedAt: now.UTC(), ClientID: clientID,
		ClientNumber: strings.TrimSpace(request.Number), ClientName: strings.TrimSpace(request.Name),
		EAVC: EAVC{SQLitePath: eavcPath, TimeZone: timeZone},
		Transport: ClientTransport{
			BotToken: request.BotToken, BotUsername: request.BotUsername,
			ServerBotUsername: request.ServerBotUsername,
			ChatID:            request.ChatID, ThreadID: request.ThreadID,
			InviteLink: strings.TrimSpace(request.InviteLink),
		},
		Cryptography: ClientCrypto{
			KeyID: keyID, KeyBase64: base64.RawStdEncoding.EncodeToString(key),
		},
		Polling: ClientPolling{
			EAVCSeconds: 10, BatchSize: 100, CatchUpExisting: request.CatchUpExisting,
			MaxEventAgeMinutes: 30,
		},
	}
	return cfg, nil
}
