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
const maxMessageTemplateRunes = 3500

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
	// Комментарии, CDATA и инструкции XML не поддерживаются Telegram.
	// Кроме того, внутри комментария можно спрятать обязательную метку.
	if strings.Contains(template, "<!") || strings.Contains(template, "<?") {
		return errors.New("в шаблоне Telegram недопустимы комментарии и инструкции XML")
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
	if len([]rune(template)) > maxMessageTemplateRunes {
		return errors.New("шаблон Telegram слишком длинный")
	}
	decoder := xml.NewDecoder(strings.NewReader("<root>" + template + "</root>"))
	var visibleText strings.Builder
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("в шаблоне Telegram нарушено HTML-оформление: %w", err)
		}
		switch value := token.(type) {
		case xml.CharData:
			visibleText.Write(value)
		case xml.StartElement:
			if value.Name.Local != "root" && (!allowedTemplateTags[value.Name.Local] || len(value.Attr) > 0) {
				return fmt.Errorf("в шаблоне Telegram недопустимый тег <%s> или его атрибуты", value.Name.Local)
			}
		case xml.Comment, xml.Directive, xml.ProcInst:
			return errors.New("в шаблоне Telegram недопустимы комментарии и инструкции XML")
		}
	}
	for _, marker := range requiredMessageMarkers {
		if !strings.Contains(visibleText.String(), marker) {
			return fmt.Errorf("обязательная метка %s отсутствует в видимом тексте шаблона Telegram", marker)
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
	remaining := strings.TrimSpace(message)
	if utf8.RuneCountInString(remaining) <= limit {
		return []string{remaining}
	}
	var chunks []string
	var current strings.Builder
	currentRunes := 0
	var stack []string
	closingRunes := 0
	for len(remaining) > 0 {
		token, tail := nextHTMLMessageToken(remaining)
		remaining = tail
		nextClosingRunes := closingRunes
		if token.name != "" {
			if token.close {
				if len(stack) > 0 {
					nextClosingRunes -= utf8.RuneCountInString(stack[len(stack)-1]) + 3
				}
			} else {
				nextClosingRunes += utf8.RuneCountInString(token.name) + 3
			}
		}
		tokenRunes := utf8.RuneCountInString(token.raw)
		if current.Len() > 0 && currentRunes+tokenRunes+nextClosingRunes > limit {
			chunks = append(chunks, current.String()+closingTags(stack))
			current.Reset()
			reopening := openingTags(stack)
			current.WriteString(reopening)
			currentRunes = utf8.RuneCountInString(reopening)
		}
		current.WriteString(token.raw)
		currentRunes += tokenRunes
		if token.name != "" {
			if token.close {
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
			} else {
				stack = append(stack, token.name)
			}
		}
		closingRunes = nextClosingRunes
	}
	if current.Len() > 0 {
		chunks = append(chunks, current.String()+closingTags(stack))
	}
	return chunks
}

func nextHTMLMessageToken(message string) (htmlMessageToken, string) {
	if message[0] == '<' {
		// В проверенном шаблоне имя тега и пробелы внутри него состоят из
		// ASCII-байтов, поэтому весь тег не длиннее шаблона. Останавливаемся
		// на первом недопустимом байте: повторяющиеся незакрытые '<' не должны
		// заставлять разбиение многократно искать далёкий '>'.
		index := 1
		close := false
		if index < len(message) && message[index] == '/' {
			close = true
			index++
		}
		nameStart := index
		for index < len(message) && index-nameStart < len("blockquote") && message[index] >= 'a' && message[index] <= 'z' {
			index++
		}
		name := message[nameStart:index]
		if allowedTemplateTags[name] {
			for index < len(message) && index < maxMessageTemplateRunes && isXMLTagSpace(message[index]) {
				index++
			}
			if index < len(message) && index < maxMessageTemplateRunes {
				if message[index] == '>' {
					return htmlMessageToken{raw: message[:index+1], name: name, close: close}, message[index+1:]
				}
				if !close && message[index] == '/' && index+1 < len(message) && index+1 < maxMessageTemplateRunes && message[index+1] == '>' {
					return htmlMessageToken{raw: message[:index+2]}, message[index+2:]
				}
			}
		}
	}
	if message[0] == '&' {
		prefix := message[:min(len(message), 16)]
		if end := strings.IndexByte(prefix, ';'); end >= 0 {
			return htmlMessageToken{raw: message[:end+1]}, message[end+1:]
		}
	}
	_, size := utf8.DecodeRuneInString(message)
	return htmlMessageToken{raw: message[:size]}, message[size:]
}

func isXMLTagSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r'
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

func runeLen(value string) int { return utf8.RuneCountInString(value) }

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
