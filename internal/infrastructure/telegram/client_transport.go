package telegram

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/protocol"
)

const (
	acknowledgementPrefixV1    = "IC-ACK/1 "
	acknowledgementPrefixV2    = "IC-ACK/2 "
	probeAcknowledgementPrefix = "IC-PROBE-ACK/1 "
	probeSchema                = "IC-PROBE/1"
)

// ClientTransport передаёт конверты точки и принимает подтверждения сервера.
type ClientTransport struct {
	API               *Client
	ChatID            int64
	ThreadID          int64
	ServerBotID       int64
	ServerBotUsername string
}

type probePayload struct {
	Schema   string `json:"schema"`
	ClientID string `json:"client_id"`
	ProbeID  string `json:"probe_id"`
}

// SendProbe отправляет безопасный служебный файл, который сервер подтверждает
// напрямую клиентскому боту. Конечные группы такой файл никогда не получают.
func (transport *ClientTransport) SendProbe(ctx context.Context, clientID string) (string, error) {
	probeID, err := protocol.UUIDv7(time.Now().UTC(), rand.Reader)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(probePayload{Schema: probeSchema, ClientID: clientID, ProbeID: probeID})
	if err != nil {
		return "", err
	}
	_, err = transport.API.SendDocument(
		ctx, transport.ChatID, transport.ThreadID, probeID+".icprobe", payload, "Проверка связи Intercept",
	)
	if err != nil {
		return "", err
	}
	return probeID, nil
}

func (transport *ClientTransport) Send(ctx context.Context, envelope protocol.Envelope) (int64, error) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return 0, fmt.Errorf("кодировать транспортный пакет: %w", err)
	}
	caption := fmt.Sprintf("IC/1 %s %s", envelope.ClientID, envelope.EventID)
	message, err := transport.API.SendDocument(
		ctx, transport.ChatID, transport.ThreadID, envelope.EventID+".icevent", payload, caption,
	)
	if err != nil {
		return 0, err
	}
	return message.MessageID, nil
}

func (transport *ClientTransport) Acknowledgements(ctx context.Context, offset int64) ([]application.Acknowledgement, int64, error) {
	updates, err := transport.API.GetUpdates(ctx, offset, 20)
	if err != nil {
		return nil, offset, err
	}
	next := offset
	var acknowledgements []application.Acknowledgement
	expected := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(transport.ServerBotUsername)), "@")
	for _, update := range updates {
		if update.UpdateID >= next {
			next = update.UpdateID + 1
		}
		message := update.Message
		if message == nil || message.From == nil || !message.From.IsBot {
			continue
		}
		if transport.ServerBotID != 0 && message.From.ID != transport.ServerBotID {
			continue
		}
		if strings.ToLower(message.From.Username) != expected {
			continue
		}
		status := "accepted"
		eventID := ""
		switch {
		case strings.HasPrefix(message.Text, probeAcknowledgementPrefix):
			eventID = strings.TrimSpace(strings.TrimPrefix(message.Text, probeAcknowledgementPrefix))
			status = "probe"
		case strings.HasPrefix(message.Text, acknowledgementPrefixV2):
			parts := strings.Fields(strings.TrimPrefix(message.Text, acknowledgementPrefixV2))
			if len(parts) != 2 || (parts[1] != "accepted" && parts[1] != "expired") {
				continue
			}
			eventID, status = parts[0], parts[1]
		case strings.HasPrefix(message.Text, acknowledgementPrefixV1):
			eventID = strings.TrimSpace(strings.TrimPrefix(message.Text, acknowledgementPrefixV1))
		default:
			continue
		}
		if eventID == "" || strings.ContainsAny(eventID, " \r\n\t") {
			continue
		}
		acknowledgements = append(acknowledgements, application.Acknowledgement{
			EventID: eventID, AcceptedAt: time.Unix(message.Date, 0).UTC(), Status: status,
		})
	}
	return acknowledgements, next, nil
}

// AcknowledgementText возвращает единый машинно-читаемый ответ сервера.
func AcknowledgementText(eventID, status string) string {
	return acknowledgementPrefixV2 + eventID + " " + status
}

func probeAcknowledgementText(probeID string) string {
	return probeAcknowledgementPrefix + probeID
}
