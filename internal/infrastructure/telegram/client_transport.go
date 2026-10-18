package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/protocol"
)

const acknowledgementPrefix = "IC-ACK/1 "

// ClientTransport передаёт конверты точки и принимает подтверждения сервера.
type ClientTransport struct {
	API               *Client
	ChatID            int64
	ThreadID          int64
	ServerBotUsername string
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
		if strings.ToLower(message.From.Username) != expected {
			continue
		}
		if !strings.HasPrefix(message.Text, acknowledgementPrefix) {
			continue
		}
		eventID := strings.TrimSpace(strings.TrimPrefix(message.Text, acknowledgementPrefix))
		if eventID == "" || strings.ContainsAny(eventID, " \r\n\t") {
			continue
		}
		acknowledgements = append(acknowledgements, application.Acknowledgement{
			EventID: eventID, AcceptedAt: time.Unix(message.Date, 0).UTC(),
		})
	}
	return acknowledgements, next, nil
}

// AcknowledgementText возвращает единый машинно-читаемый ответ сервера.
func AcknowledgementText(eventID string) string {
	return acknowledgementPrefix + eventID
}
