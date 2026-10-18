package telegram

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"intercept/internal/domain"
)

// FormatEvent сохраняет вид сообщений рабочей Python-версии.
func FormatEvent(event domain.Event, review bool, notes ...string) string {
	block := domain.ContentBlock{Text: event.Content}
	if len(event.Blocks) > 0 {
		block = event.Blocks[0]
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver}}
	if review {
		delivery.Decision.Action = domain.RouteReview
	}
	if len(notes) > 0 {
		delivery.Decision.Note = notes[0]
	}
	return formatBlock(event, block, delivery, "")
}

func formatBlockChunks(event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery) []string {
	parts := splitRunes(block.Text, 3000)
	if len(parts) == 0 {
		return nil
	}
	chunks := make([]string, 0, len(parts))
	for index, part := range parts {
		suffix := ""
		if len(parts) > 1 {
			suffix = fmt.Sprintf("\n\n(%d/%d)", index+1, len(parts))
		}
		copyBlock := block
		copyBlock.Text = part
		chunks = append(chunks, formatBlock(event, copyBlock, delivery, suffix))
	}
	return chunks
}

func formatBlock(event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery, suffix string) string {
	var builder strings.Builder
	if delivery.Decision.Action == domain.RouteReview {
		builder.WriteString("🔴 НОВОЕ ИЛИ ОШИБОЧНОЕ ПОДРАЗДЕЛЕНИЕ\n")
		if note := strings.TrimSpace(delivery.Decision.Note); note != "" {
			builder.WriteString(escapeText(note))
			builder.WriteString("\n")
		}
		builder.WriteString("──────────────\n")
	}
	builder.WriteString("ОВЧ радиосеть\n<b>")
	builder.WriteString(escapeText(fallbackText(event.Position.UnitName, "—")))
	builder.WriteString("</b>\nЧастота ")
	builder.WriteString(escapeText(fallbackText(event.Position.Frequency, "—")))
	builder.WriteString(" МГц (групповой ID ")
	builder.WriteString(escapeText(fallbackText(event.Position.GroupCode, "—")))
	builder.WriteString(" AES-256)\nДата: ")
	builder.WriteString(eventDate(event).Format("02.01.2006"))
	builder.WriteString("\n\n")
	callsigns := block.Callsigns
	if len(callsigns) == 0 {
		for _, code := range domain.ExtractCorrespondentCodes(block.Text) {
			callsigns = append(callsigns, domain.Callsign{Code: code, Label: "н/у"})
		}
	}
	seen := make(map[string]bool)
	for _, callsign := range callsigns {
		code := strings.TrimSpace(callsign.Code)
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		builder.WriteString("<b>")
		builder.WriteString(escapeText(fallbackText(callsign.Label, "н/у")))
		builder.WriteString(" (")
		builder.WriteString(escapeText(code))
		builder.WriteString(")</b>\n")
	}
	if len(seen) > 0 {
		builder.WriteString("\n")
	}
	builder.WriteString(escapeText(strings.TrimSpace(block.Text)))
	builder.WriteString(suffix)
	return builder.String()
}

func eventDate(event domain.Event) time.Time {
	date := event.SourceAt
	if date.IsZero() {
		date = event.ObservedAt
	}
	if date.IsZero() {
		date = event.CreatedAt
	}
	return date.In(time.FixedZone("MSK", 3*60*60))
}

func splitRunes(value string, limit int) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	runes := []rune(value)
	var result []string
	for len(runes) > limit {
		cut := limit
		for cut > limit/2 && runes[cut] != '\n' {
			cut--
		}
		if cut <= limit/2 {
			cut = limit
		}
		result = append(result, strings.TrimSpace(string(runes[:cut])))
		runes = runes[cut:]
	}
	if tail := strings.TrimSpace(string(runes)); tail != "" {
		result = append(result, tail)
	}
	return result
}

func hashChunks(chunks []string) string {
	digest := sha256.Sum256([]byte(strings.Join(chunks, "\x00")))
	return fmt.Sprintf("%x", digest[:])
}

func fallbackText(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func escapeText(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	return strings.ReplaceAll(value, ">", "&gt;")
}
