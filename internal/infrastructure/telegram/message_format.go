package telegram

import (
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"intercept/internal/domain"
)

var legacyDatePrefix = regexp.MustCompile(`^\s*(\d{4})-(\d{2})-(\d{2})`)

// DefaultMessageTemplate — редактируемый шаблон сообщения получателя.
// Служебная пометка проверки и номер части длинного сообщения добавляются
// программой снаружи, поэтому их невозможно случайно удалить из шаблона.
const DefaultProtection = "AES-256"

const DefaultMessageTemplate = `Радиосеть <b>{{UNIT}}</b> - {{NETWORK}}
{{FREQUENCY}} МГц (гр. ID {{GROUP_ID}} {{PROTECTION}})
{{DATE}}г.

{{CORRESPONDENTS}}

{{TRANSCRIPT}}`

var requiredMessageMarkers = []string{
	"{{UNIT}}", "{{NETWORK}}", "{{FREQUENCY}}", "{{GROUP_ID}}",
	"{{PROTECTION}}", "{{DATE}}", "{{CORRESPONDENTS}}", "{{TRANSCRIPT}}",
}

const maxTelegramMessageRunes = 4096

var allowedTemplateTags = map[string]bool{
	"b": true, "strong": true, "i": true, "em": true, "u": true, "ins": true,
	"s": true, "strike": true, "del": true, "code": true, "pre": true, "blockquote": true,
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
	if len([]rune(template)) > 3500 {
		return errors.New("шаблон Telegram слишком длинный")
	}
	decoder := xml.NewDecoder(strings.NewReader("<root>" + template + "</root>"))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("в шаблоне Telegram нарушено HTML-оформление: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local == "root" {
			continue
		}
		if !allowedTemplateTags[start.Name.Local] || len(start.Attr) > 0 {
			return fmt.Errorf("в шаблоне Telegram недопустимый тег <%s> или его атрибуты", start.Name.Local)
		}
	}
	return nil
}

// MigrateMessageTemplate переводит старый редактируемый шаблон на явную метку
// защиты. Значение защиты хранится в настройках получателя, а не в коде макета.
func MigrateMessageTemplate(template string) string {
	if strings.Contains(template, "МГц (ID {{GROUP_ID}}") {
		template = strings.Replace(template, "МГц (ID {{GROUP_ID}}", "МГц (гр. ID {{GROUP_ID}}", 1)
	}
	if strings.Contains(template, "{{PROTECTION}}") {
		return template
	}
	if strings.Contains(template, "AES-256") {
		return strings.Replace(template, "AES-256", "{{PROTECTION}}", 1)
	}
	return strings.Replace(template, "{{GROUP_ID}}", "{{GROUP_ID}} {{PROTECTION}}", 1)
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
	block := combinedEventBlock(event)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver}}
	if review {
		delivery.Decision.Action = domain.RouteReview
	}
	if len(notes) > 0 {
		delivery.Decision.Note = notes[0]
	}
	return formatBlock(event, block, delivery, network, DefaultProtection, template, "")
}

// combinedEventBlock сохраняет одну запись EAVC одним сообщением: общий список
// корреспондентов выводится один раз, а временные части идут подряд в исходном
// порядке. Разделение на несколько сообщений остаётся только защитой от лимита
// длины Telegram и выполняется позже в formatBlockChunks.
func combinedEventBlock(event domain.Event) domain.ContentBlock {
	blocks := append([]domain.ContentBlock(nil), event.Blocks...)
	if len(blocks) == 0 {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	if len(blocks) == 0 {
		return domain.ContentBlock{Text: event.Content}
	}
	sort.SliceStable(blocks, func(left, right int) bool { return blocks[left].Sequence < blocks[right].Sequence })
	texts := make([]string, 0, len(blocks))
	callsigns := make([]domain.Callsign, 0)
	for _, block := range blocks {
		if text := strings.TrimSpace(block.Text); text != "" {
			texts = append(texts, text)
		}
		for _, callsign := range block.Callsigns {
			callsigns = append(callsigns, callsign)
		}
	}
	return domain.ContentBlock{
		Key: "record", Text: strings.Join(texts, "\n\n"), Callsigns: callsigns,
	}
}

func formatEventChunks(event domain.Event, delivery domain.PlannedDelivery, network, protection, template string) []string {
	return formatEventChunksWithEditedAt(event, delivery, network, protection, template, time.Time{})
}

func formatBlockChunks(
	event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery,
	network, protection, template string,
) []string {
	return formatBlockChunksWithEditedAt(event, block, delivery, network, protection, template, time.Time{})
}

func formatBlockChunksWithEditedAt(
	event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery,
	network, protection, template string, editedAt time.Time,
) []string {
	suffix := ""
	if !editedAt.IsZero() {
		suffix = "\n\n(отредактировано " + editedAt.In(time.FixedZone("MSK", 3*60*60)).Format("15:04:05") + ")"
	}
	message := formatBlock(event, block, delivery, network, protection, template, suffix)
	parts := splitHTMLMessage(message, maxTelegramMessageRunes-48)
	if len(parts) <= 1 {
		return parts
	}
	for index := range parts {
		parts[index] += fmt.Sprintf("\n\n(часть %d из %d)", index+1, len(parts))
	}
	return parts
}

// formatEventChunksWithEditedAt добавляет служебную отметку в самый конец
// перехвата. Она не является репликой корреспондента и остаётся видимой даже
// в клиентах Telegram, которые не показывают собственный признак правки.
func formatEventChunksWithEditedAt(
	event domain.Event,
	delivery domain.PlannedDelivery,
	network, protection, template string,
	editedAt time.Time,
) []string {
	return formatBlockChunksWithEditedAt(
		event, combinedEventBlock(event), delivery, network, protection, template, editedAt,
	)
}

func formatBlock(event domain.Event, block domain.ContentBlock, delivery domain.PlannedDelivery, network, protection, template, suffix string) string {
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
		correspondentText.WriteString(fmt.Sprintf("%d.«<b>", correspondent.Number))
		correspondentText.WriteString(escapeText(correspondent.Label))
		correspondentText.WriteString("</b>» (id: ")
		correspondentText.WriteString(escapeText(correspondent.Code))
		correspondentText.WriteString(")")
		if description != "" {
			correspondentText.WriteString(" – ")
			correspondentText.WriteString(escapeText(strings.TrimRight(description, ";")))
		}
		correspondentText.WriteString(";")
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
		if line.InternalTime {
			transcript.WriteString("(")
		}
		transcript.WriteString(escapeText(line.Text))
		if line.InternalTime {
			transcript.WriteString(")")
		}
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
		"{{PROTECTION}}":     escapeText(fallbackText(protection, "—")),
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

type htmlMessageToken struct {
	raw   string
	name  string
	close bool
}

func splitHTMLMessage(message string, limit int) []string {
	tokens := tokenizeHTMLMessage(strings.TrimSpace(message))
	if len([]rune(message)) <= limit {
		return []string{strings.TrimSpace(message)}
	}
	var chunks []string
	var current strings.Builder
	var stack []string
	for _, token := range tokens {
		nextStack := append([]string(nil), stack...)
		if token.name != "" {
			if token.close {
				if len(nextStack) > 0 {
					nextStack = nextStack[:len(nextStack)-1]
				}
			} else {
				nextStack = append(nextStack, token.name)
			}
		}
		if current.Len() > 0 && runeLen(current.String()+token.raw+closingTags(nextStack)) > limit {
			chunks = append(chunks, strings.TrimSpace(current.String()+closingTags(stack)))
			current.Reset()
			current.WriteString(openingTags(stack))
		}
		current.WriteString(token.raw)
		stack = nextStack
	}
	if tail := strings.TrimSpace(current.String() + closingTags(stack)); tail != "" {
		chunks = append(chunks, tail)
	}
	return chunks
}

func tokenizeHTMLMessage(message string) []htmlMessageToken {
	var result []htmlMessageToken
	for len(message) > 0 {
		if message[0] == '<' {
			if end := strings.IndexByte(message, '>'); end >= 0 {
				raw := message[:end+1]
				name := strings.Trim(strings.TrimSuffix(strings.TrimPrefix(raw, "<"), ">"), " /")
				result = append(result, htmlMessageToken{raw: raw, name: name, close: strings.HasPrefix(raw, "</")})
				message = message[end+1:]
				continue
			}
		}
		if message[0] == '&' {
			if end := strings.IndexByte(message, ';'); end >= 0 && end < 16 {
				result = append(result, htmlMessageToken{raw: message[:end+1]})
				message = message[end+1:]
				continue
			}
		}
		_, size := utf8.DecodeRuneInString(message)
		result = append(result, htmlMessageToken{raw: message[:size]})
		message = message[size:]
	}
	return result
}

func openingTags(stack []string) string {
	var builder strings.Builder
	for _, name := range stack {
		builder.WriteString("<" + name + ">")
	}
	return builder.String()
}

func closingTags(stack []string) string {
	var builder strings.Builder
	for index := len(stack) - 1; index >= 0; index-- {
		builder.WriteString("</" + stack[index] + ">")
	}
	return builder.String()
}

func runeLen(value string) int { return len([]rune(value)) }

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
