package telegram

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"intercept/internal/domain"
)

var legacyDatePrefix = regexp.MustCompile(`^\s*(\d{4})-(\d{2})-(\d{2})`)

// DefaultMessageTemplate — редактируемый шаблон сообщения получателя.
// Служебная пометка проверки и номер части длинного сообщения добавляются
// программой снаружи, поэтому их невозможно случайно удалить из шаблона.
const DefaultMessageTemplate = `Радиосеть <b>{{UNIT}}</b> - {{NETWORK}}
{{FREQUENCY}} МГц (ID {{GROUP_ID}} AES-256)
{{DATE}}г.

{{CORRESPONDENTS}}

{{TRANSCRIPT}}`

var requiredMessageMarkers = []string{
	"{{UNIT}}", "{{NETWORK}}", "{{FREQUENCY}}", "{{GROUP_ID}}",
	"{{DATE}}", "{{CORRESPONDENTS}}", "{{TRANSCRIPT}}",
}

// MessageTemplateMarkers возвращает обязательные метки для подсказки в интерфейсе.
func MessageTemplateMarkers() []string {
	return append([]string(nil), requiredMessageMarkers...)
}

// ValidateMessageTemplate не позволяет сохранить шаблон, который потеряет
// обязательные данные перехвата.
func ValidateMessageTemplate(template string) error {
	template = strings.TrimSpace(template)
	if template == "" {
		return errors.New("шаблон сообщения Telegram пуст")
	}
	var missing []string
	for _, marker := range requiredMessageMarkers {
		if !strings.Contains(template, marker) {
			missing = append(missing, marker)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("в шаблоне Telegram отсутствуют обязательные метки: %s", strings.Join(missing, ", "))
	}
	return nil
}

// FormatEvent формирует сообщение без дополнительного обозначения радиосети.
func FormatEvent(event domain.Event, review bool, notes ...string) string {
	return FormatEventForNetwork(event, "", review, notes...)
}

// FormatEventForNetwork формирует сообщение по утверждённому образцу Telegram.
func FormatEventForNetwork(event domain.Event, network string, review bool, notes ...string) string {
	return FormatEventWithTemplate(event, network, DefaultMessageTemplate, review, notes...)
}

// FormatEventWithTemplate формирует сообщение по выбранному пользователем шаблону.
func FormatEventWithTemplate(event domain.Event, network, template string, review bool, notes ...string) string {
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
	return formatBlock(event, block, delivery, network, template, "")
}

func formatBlockChunks(event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery, network, template string) []string {
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
		chunks = append(chunks, formatBlock(event, copyBlock, delivery, network, template, suffix))
	}
	return chunks
}

func formatBlock(event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery, network, template, suffix string) string {
	var builder strings.Builder
	if delivery.Decision.Action == domain.RouteReview {
		builder.WriteString("🔴 НОВОЕ ИЛИ ОШИБОЧНОЕ ПОДРАЗДЕЛЕНИЕ\n")
		if note := strings.TrimSpace(delivery.Decision.Note); note != "" {
			builder.WriteString(escapeText(note))
			builder.WriteString("\n")
		}
		builder.WriteString("──────────────\n")
	}
	callsigns := block.Callsigns
	if len(callsigns) == 0 {
		for _, code := range domain.ExtractCorrespondentCodes(block.Text) {
			callsigns = append(callsigns, domain.Callsign{Code: code, Label: "н/у"})
		}
	}
	correspondents, lines := domain.PresentIntercept(block.Text, callsigns)
	var correspondentText strings.Builder
	for _, correspondent := range correspondents {
		description := strings.TrimSpace(correspondent.Description)
		if description == "" {
			description = "военнослужащий " + fallbackText(event.Position.UnitName, "—") + ", мужской голос, украинская речь"
		}
		description = strings.TrimRight(description, ";") + ";"
		correspondentText.WriteString(fmt.Sprintf("%d. «<b>", correspondent.Number))
		correspondentText.WriteString(escapeText(correspondent.Label))
		correspondentText.WriteString("</b>» (id: ")
		correspondentText.WriteString(escapeText(correspondent.Code))
		correspondentText.WriteString(") ")
		correspondentText.WriteString(escapeText(description))
		correspondentText.WriteString("\n")
	}
	var transcript strings.Builder
	for index, line := range lines {
		if index > 0 {
			transcript.WriteString("\n")
		}
		if line.Time {
			transcript.WriteString("<b>")
		}
		transcript.WriteString(escapeText(line.Text))
		if line.Time {
			transcript.WriteString("</b>")
		}
	}
	if strings.TrimSpace(template) == "" {
		template = DefaultMessageTemplate
	}
	replacements := map[string]string{
		"{{UNIT}}":           escapeText(fallbackText(event.Position.UnitName, "—")),
		"{{NETWORK}}":        escapeText(fallbackText(network, "—")),
		"{{FREQUENCY}}":      escapeText(fallbackText(event.Position.Frequency, "—")),
		"{{GROUP_ID}}":       escapeText(fallbackText(event.Position.GroupCode, "—")),
		"{{DATE}}":           eventDate(event),
		"{{CORRESPONDENTS}}": strings.TrimRight(correspondentText.String(), "\n"),
		"{{TRANSCRIPT}}":     transcript.String(),
	}
	message := template
	for _, marker := range requiredMessageMarkers {
		message = strings.ReplaceAll(message, marker, replacements[marker])
	}
	builder.WriteString(strings.TrimSpace(message))
	builder.WriteString(suffix)
	return builder.String()
}

func eventDate(event domain.Event) string {
	if match := legacyDatePrefix.FindStringSubmatch(event.Source.UpdatedAt); match != nil {
		return match[3] + "." + match[2] + "." + match[1]
	}
	date := event.SourceAt
	if date.IsZero() {
		date = event.ObservedAt
	}
	if date.IsZero() {
		date = event.CreatedAt
	}
	return date.In(time.FixedZone("MSK", 3*60*60)).Format("02.01.2006")
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
