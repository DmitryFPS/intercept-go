package telegram

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"intercept/internal/application"
)

// ServerReceiver получает только файлы из настроенной транспортной группы.
type ServerReceiver struct {
	API      *Client
	ChatID   int64
	MaxBytes int64
}

// Packets возвращает пакеты и следующее безопасное смещение Telegram.
func (receiver *ServerReceiver) Packets(ctx context.Context, offset int64) ([]application.IncomingPacket, int64, error) {
	updates, err := receiver.API.GetUpdates(ctx, offset, 25)
	if err != nil {
		return nil, offset, err
	}
	next := offset
	var packets []application.IncomingPacket
	for _, update := range updates {
		if update.UpdateID >= next {
			next = update.UpdateID + 1
		}
		message := update.Message
		if message == nil {
			message = update.ChannelPost
		}
		if message == nil || message.Chat.ID != receiver.ChatID || message.Document == nil || message.From == nil {
			continue
		}
		if !message.From.IsBot || !strings.HasSuffix(strings.ToLower(message.Document.FileName), ".icevent") {
			continue
		}
		if message.Document.FileSize <= 0 || message.Document.FileSize > receiver.MaxBytes {
			continue
		}
		file, err := receiver.API.GetFile(ctx, message.Document.FileID)
		if err != nil {
			return packets, offset, fmt.Errorf("получить пакет Telegram: %w", err)
		}
		payload, err := receiver.API.DownloadFile(ctx, file.FilePath, receiver.MaxBytes)
		if err != nil {
			return packets, offset, err
		}
		packets = append(packets, application.IncomingPacket{
			SenderBotID: message.From.ID, SenderBotUsername: message.From.Username,
			ChatID: message.Chat.ID, ThreadID: message.MessageThreadID,
			MessageID: message.MessageID, Payload: payload,
		})
	}
	return packets, next, nil
}

// DeletePacket удаляет уже сохранённый транспортный файл из служебной группы.
// Ошибка удаления не влияет на приём: локальная дедупликация остаётся источником истины.
func (receiver *ServerReceiver) DeletePacket(ctx context.Context, messageID int64) error {
	if receiver.API == nil || receiver.ChatID == 0 || messageID <= 0 {
		return errors.New("не заданы данные транспортного сообщения")
	}
	return receiver.API.DeleteMessage(ctx, receiver.ChatID, messageID)
}

// Acknowledge сообщает боту точки, что событие записано сервером.
func (receiver *ServerReceiver) Acknowledge(ctx context.Context, botUsername, eventID, status string) error {
	if strings.TrimSpace(botUsername) == "" || strings.TrimSpace(eventID) == "" {
		return errors.New("для подтверждения не задан бот или событие")
	}
	if status != "accepted" && status != "expired" {
		return errors.New("для подтверждения задан неизвестный статус")
	}
	_, err := receiver.API.SendMessage(ctx, 0, botUsername, AcknowledgementText(eventID, status))
	return err
}
