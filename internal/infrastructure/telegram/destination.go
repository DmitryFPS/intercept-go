package telegram

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"intercept/internal/domain"
)

// Destination публикует одно событие в одну Telegram-группу или канал.
type Destination struct {
	API    *Client
	ChatID int64
}

func (destination *Destination) Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error {
	text := FormatEvent(event, delivery.Decision.Action == domain.RouteReview, delivery.Decision.Note)
	if utf8.RuneCountInString(text) <= 4000 {
		_, err := destination.API.SendText(ctx, destination.ChatID, delivery.Decision.ThreadID, text)
		return err
	}
	filename := safeTextFilename(event.Position.UnitName) + ".txt"
	caption := fmt.Sprintf("%s · событие %s", event.Position.UnitName, event.ID)
	_, err := destination.API.SendDocument(
		ctx, destination.ChatID, delivery.Decision.ThreadID, filename, []byte(text), caption,
	)
	return err
}

// FormatEvent формирует понятное человеку сообщение без технических ключей.
func FormatEvent(event domain.Event, review bool, notes ...string) string {
	var builder strings.Builder
	if review {
		builder.WriteString("🔴 НОВОЕ ИЛИ ОШИБОЧНОЕ ПОДРАЗДЕЛЕНИЕ\n\n")
	}
	builder.WriteString("Подразделение: ")
	builder.WriteString(event.Position.UnitName)
	builder.WriteString("\n")
	if event.Position.Frequency != "" {
		builder.WriteString("Частота: ")
		builder.WriteString(event.Position.Frequency)
		builder.WriteString(" МГц\n")
	}
	if event.Position.GroupCode != "" {
		builder.WriteString("Группа: ")
		builder.WriteString(event.Position.GroupCode)
		builder.WriteString("\n")
	}
	if event.Mode == domain.SourceModeDeferred {
		builder.WriteString("Режим: отложенный\n")
	}
	if len(notes) > 0 && strings.TrimSpace(notes[0]) != "" {
		builder.WriteString("Проверка: ")
		builder.WriteString(strings.TrimSpace(notes[0]))
		builder.WriteString("\n")
	}
	builder.WriteString("\n")
	builder.WriteString(event.Content)
	return builder.String()
}

func safeTextFilename(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Map(func(char rune) rune {
		switch char {
		case '<', '>', ':', '"', '/', '\\', '|', '?', '*':
			return '-'
		default:
			return char
		}
	}, value)
	if value == "" {
		return "перехват"
	}
	if utf8.RuneCountInString(value) > 80 {
		return string([]rune(value)[:80])
	}
	return value
}
