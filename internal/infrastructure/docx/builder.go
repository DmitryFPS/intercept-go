package docx

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"intercept/internal/domain"
)

var (
	legacyDatePrefix = regexp.MustCompile(`^\s*(\d{4})-(\d{2})-(\d{2})`)
	paragraphPattern = regexp.MustCompile(`(?s)<w:p(?:\s[^>]*)?>.*?</w:p>`)
	textPattern      = regexp.MustCompile(`(?s)<w:t(?:\s[^>]*)?>(.*?)</w:t>`)
	paragraphProps   = regexp.MustCompile(`(?s)<w:pPr(?:\s[^>]*)?>.*?</w:pPr>`)
	runProps         = regexp.MustCompile(`(?s)<w:rPr(?:\s[^>]*)?>(.*?)</w:rPr>`)
	boldTag          = regexp.MustCompile(`(?s)<w:b(?:Cs)?(?:\s[^>]*)?/>`)
)

const (
	markerUnit            = "{{UNIT}}"
	markerNetwork         = "{{NETWORK}}"
	markerFrequency       = "{{FREQUENCY}}"
	markerGroupID         = "{{GROUP_ID}}"
	markerProtection      = "{{PROTECTION}}"
	markerDate            = "{{DATE}}"
	markerCorrespondents  = "{{CORRESPONDENTS}}"
	markerTranscript      = "{{TRANSCRIPT}}"
	markerEdited          = "{{EDITED_MARK}}"
	maxTemplateParts      = 2048
	maxTemplatePartBytes  = 16 * 1024 * 1024
	maxTemplateTotalBytes = 64 * 1024 * 1024
)

var requiredTemplateMarkers = []string{
	markerUnit, markerNetwork, markerFrequency, markerGroupID, markerDate,
	markerCorrespondents, markerTranscript, markerEdited,
}

var blockTemplateMarkers = map[string]bool{
	markerCorrespondents: true,
	markerTranscript:     true,
	markerEdited:         true,
}

// Document содержит данные одного Word-документа без привязки к файлам.
type Document struct {
	Unit            string
	Frequency       string
	GroupCode       string
	Network         string
	Protection      string
	Date            time.Time
	Operator        string
	Content         string
	Callsigns       []domain.Callsign
	SourceUpdatedAt string
	Edited          bool
	EditedAt        time.Time
}

// TemplateMarkers возвращает обязательные метки для подсказки в интерфейсе.
func TemplateMarkers() []string {
	return append([]string(nil), requiredTemplateMarkers...)
}

// ValidateTemplate проверяет структуру DOCX и наличие всех обязательных меток.
func ValidateTemplate(templatePath string) error {
	parts, _, err := readTemplate(templatePath)
	if err != nil {
		return err
	}
	if err := validateSafeTemplateParts(parts); err != nil {
		return err
	}
	documentXML, exists := parts["word/document.xml"]
	if !exists {
		return errors.New("шаблон Word не содержит word/document.xml")
	}
	return validateTemplateXML(string(documentXML))
}

// Build сохраняет оформление и содержимое шаблона, заменяя только метки.
func Build(templatePath string, document Document) ([]byte, error) {
	parts, order, err := readTemplate(templatePath)
	if err != nil {
		return nil, err
	}
	if err := validateSafeTemplateParts(parts); err != nil {
		return nil, err
	}
	original, exists := parts["word/document.xml"]
	if !exists {
		return nil, errors.New("шаблон Word не содержит word/document.xml")
	}
	if err := validateTemplateXML(string(original)); err != nil {
		return nil, err
	}
	rendered, err := renderTemplateXML(string(original), document)
	if err != nil {
		return nil, err
	}
	parts["word/document.xml"] = []byte(rendered)

	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for _, name := range order {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		part, err := writer.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("создать часть DOCX %s: %w", name, err)
		}
		if _, err := part.Write(parts[name]); err != nil {
			return nil, fmt.Errorf("записать часть DOCX %s: %w", name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("завершить DOCX: %w", err)
	}
	return output.Bytes(), nil
}

func readTemplate(templatePath string) (map[string][]byte, []string, error) {
	archive, err := zip.OpenReader(templatePath)
	if err != nil {
		return nil, nil, fmt.Errorf("открыть шаблон Word: %w", err)
	}
	defer archive.Close()
	if len(archive.File) > maxTemplateParts {
		return nil, nil, fmt.Errorf("шаблон Word содержит слишком много частей: %d", len(archive.File))
	}
	parts := make(map[string][]byte, len(archive.File))
	order := make([]string, 0, len(archive.File))
	var totalSize uint64
	for _, file := range archive.File {
		name, err := safeDOCXPartName(file.Name)
		if err != nil {
			return nil, nil, err
		}
		if file.UncompressedSize64 > maxTemplatePartBytes || totalSize+file.UncompressedSize64 > maxTemplateTotalBytes {
			return nil, nil, fmt.Errorf("шаблон Word слишком велик после распаковки: %s", name)
		}
		if _, duplicate := parts[name]; duplicate {
			return nil, nil, fmt.Errorf("шаблон Word содержит повторяющуюся часть: %s", name)
		}
		reader, err := file.Open()
		if err != nil {
			return nil, nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(reader, maxTemplatePartBytes+1))
		reader.Close()
		if readErr != nil {
			return nil, nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, readErr)
		}
		if len(payload) > maxTemplatePartBytes {
			return nil, nil, fmt.Errorf("часть шаблона Word слишком велика: %s", file.Name)
		}
		totalSize += uint64(len(payload))
		parts[name] = payload
		order = append(order, name)
	}
	return parts, order, nil
}

func safeDOCXPartName(value string) (string, error) {
	value = strings.ReplaceAll(strings.TrimSpace(value), "\\", "/")
	clean := filepath.ToSlash(filepath.Clean(value))
	if value == "" || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") || strings.Contains(clean, ":") {
		return "", fmt.Errorf("шаблон Word содержит небезопасный путь: %q", value)
	}
	return clean, nil
}

func validateSafeTemplateParts(parts map[string][]byte) error {
	for name, payload := range parts {
		if !allowedDOCXPart(name) {
			return fmt.Errorf("шаблон Word содержит неподдерживаемую активную часть: %s", name)
		}
		lowerPayload := strings.ToLower(string(payload))
		if strings.HasSuffix(strings.ToLower(name), ".rels") {
			if err := validateRelationships(payload, name); err != nil {
				return err
			}
		}
		if strings.HasSuffix(strings.ToLower(name), ".xml") &&
			(strings.Contains(lowerPayload, "attachedtemplate") || strings.Contains(lowerPayload, "oleobject") || strings.Contains(lowerPayload, "activex")) {
			return fmt.Errorf("шаблон Word содержит активное содержимое: %s", name)
		}
		if name == "[Content_Types].xml" &&
			(strings.Contains(lowerPayload, "macroenabled") || strings.Contains(lowerPayload, "vbaproject") || strings.Contains(lowerPayload, "activex") || strings.Contains(lowerPayload, "oleobject")) {
			return errors.New("шаблон Word содержит макросы или активные объекты")
		}
	}
	return nil
}

func allowedDOCXPart(name string) bool {
	lower := strings.ToLower(name)
	if lower == "[content_types].xml" || lower == "_rels/.rels" {
		return true
	}
	if strings.HasPrefix(lower, "docprops/") && strings.HasSuffix(lower, ".xml") {
		return true
	}
	if strings.HasPrefix(lower, "word/_rels/") && strings.HasSuffix(lower, ".rels") {
		return true
	}
	if strings.HasPrefix(lower, "word/theme/") && strings.HasSuffix(lower, ".xml") {
		return true
	}
	if strings.HasPrefix(lower, "word/media/") {
		switch filepath.Ext(lower) {
		case ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff":
			return true
		}
	}
	if !strings.HasPrefix(lower, "word/") || !strings.HasSuffix(lower, ".xml") {
		return false
	}
	base := strings.TrimSuffix(strings.TrimPrefix(lower, "word/"), ".xml")
	for _, allowed := range []string{"document", "styles", "settings", "fonttable", "websettings", "numbering", "footnotes", "endnotes", "comments"} {
		if base == allowed {
			return true
		}
	}
	return (strings.HasPrefix(base, "header") || strings.HasPrefix(base, "footer")) && allDigits(strings.TrimLeft(base, "abcdefghijklmnopqrstuvwxyz"))
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, symbol := range value {
		if symbol < '0' || symbol > '9' {
			return false
		}
	}
	return true
}

func validateRelationships(payload []byte, part string) error {
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("прочитать связи шаблона %s: %w", part, err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "Relationship" {
			continue
		}
		for _, attribute := range start.Attr {
			if attribute.Name.Local == "TargetMode" && strings.EqualFold(strings.TrimSpace(attribute.Value), "External") {
				return fmt.Errorf("шаблон Word содержит внешнюю связь: %s", part)
			}
			if attribute.Name.Local == "Type" {
				value := strings.ToLower(attribute.Value)
				if strings.Contains(value, "attachedtemplate") || strings.Contains(value, "oleobject") || strings.Contains(value, "activex") || strings.Contains(value, "vbaproject") {
					return fmt.Errorf("шаблон Word содержит активную связь: %s", part)
				}
			}
		}
	}
}

func validateTemplateXML(payload string) error {
	paragraphs := paragraphPattern.FindAllString(payload, -1)
	present := make(map[string]bool, len(requiredTemplateMarkers))
	invalidBlocks := make(map[string]bool)
	for _, paragraph := range paragraphs {
		text := paragraphText(paragraph)
		for _, marker := range requiredTemplateMarkers {
			if strings.Contains(text, marker) {
				present[marker] = true
				if blockTemplateMarkers[marker] && strings.TrimSpace(text) != marker {
					invalidBlocks[marker] = true
				}
			}
		}
	}
	var missing []string
	for _, marker := range requiredTemplateMarkers {
		if !present[marker] {
			missing = append(missing, marker)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("в шаблоне Word отсутствуют обязательные метки: %s", strings.Join(missing, ", "))
	}
	if len(invalidBlocks) > 0 {
		var markers []string
		for marker := range invalidBlocks {
			markers = append(markers, marker)
		}
		sort.Strings(markers)
		return fmt.Errorf("метки %s должны находиться каждая в отдельном абзаце", strings.Join(markers, ", "))
	}
	return nil
}

func renderTemplateXML(payload string, document Document) (string, error) {
	if document.Edited && document.EditedAt.IsZero() {
		return "", errors.New("для отредактированного документа не задано время изменения")
	}
	correspondents, lines := domain.PresentIntercept(document.Content, document.Callsigns)
	payload = replaceBlock(payload, markerCorrespondents, func(paragraph string) string {
		var result strings.Builder
		for _, correspondent := range correspondents {
			description := strings.TrimSpace(correspondent.Description)
			if description == "" {
				description = "сведения не указаны"
			}
			description = strings.TrimRight(description, ";") + ";"
			result.WriteString(renderParagraph(paragraph, []styledRun{
				{Text: strconv.Itoa(correspondent.Number) + ".«"},
				{Text: correspondent.Label, Bold: true},
				{Text: "» (id: " + correspondent.Code + ") – " + description},
			}))
		}
		if len(correspondents) > 0 {
			result.WriteString(renderParagraph(paragraph, nil))
		}
		return result.String()
	})
	payload = replaceBlock(payload, markerTranscript, func(paragraph string) string {
		var result strings.Builder
		for _, line := range lines {
			result.WriteString(renderParagraph(paragraph, []styledRun{{Text: line.Text, Bold: line.Time}}))
		}
		return result.String()
	})
	payload = replaceBlock(payload, markerEdited, func(paragraph string) string {
		if !document.Edited {
			return ""
		}
		return renderParagraph(paragraph, nil) + renderParagraph(paragraph, []styledRun{{Text: editedMark(document.EditedAt)}})
	})

	date := document.Date
	if date.IsZero() {
		date = time.Now()
	}
	dateText := date.In(time.FixedZone("MSK", 3*60*60)).Format("02.01.2006")
	if match := legacyDatePrefix.FindStringSubmatch(document.SourceUpdatedAt); match != nil {
		dateText = match[3] + "." + match[2] + "." + match[1]
	}
	replacements := map[string]string{
		markerUnit:       fallback(document.Unit, "н/у"),
		markerNetwork:    fallback(document.Network, "—"),
		markerFrequency:  fallback(document.Frequency, "—"),
		markerGroupID:    fallback(document.GroupCode, "—"),
		markerProtection: fallback(document.Protection, "—"),
		markerDate:       dateText,
	}
	for marker, value := range replacements {
		var replaced bool
		payload, replaced = replaceScalarMarker(payload, marker, value)
		if !replaced {
			return "", fmt.Errorf("не удалось заполнить метку Word %s", marker)
		}
	}
	payload = strings.ReplaceAll(payload, "AES-256", escapeXMLText(fallback(document.Protection, "—")))
	return payload, nil
}

func editedMark(editedAt time.Time) string {
	return "(отредактировано " + editedAt.In(time.FixedZone("MSK", 3*60*60)).Format("15:04:05") + ")"
}

func escapeXMLText(value string) string {
	var builder strings.Builder
	_ = xml.EscapeText(&builder, []byte(value))
	return builder.String()
}

func replaceBlock(payload, marker string, render func(string) string) string {
	indexes := paragraphPattern.FindAllStringIndex(payload, -1)
	for index := len(indexes) - 1; index >= 0; index-- {
		bounds := indexes[index]
		paragraph := payload[bounds[0]:bounds[1]]
		if strings.TrimSpace(paragraphText(paragraph)) != marker {
			continue
		}
		payload = payload[:bounds[0]] + render(paragraph) + payload[bounds[1]:]
	}
	return payload
}

func replaceScalarMarker(payload, marker, value string) (string, bool) {
	indexes := paragraphPattern.FindAllStringIndex(payload, -1)
	replaced := false
	for index := len(indexes) - 1; index >= 0; index-- {
		bounds := indexes[index]
		paragraph := payload[bounds[0]:bounds[1]]
		count := strings.Count(paragraphText(paragraph), marker)
		if count == 0 {
			continue
		}
		for occurrence := 0; occurrence < count; occurrence++ {
			paragraph, _ = replaceMarkerInParagraph(paragraph, marker, value)
		}
		payload = payload[:bounds[0]] + paragraph + payload[bounds[1]:]
		replaced = true
	}
	return payload, replaced
}

type textNode struct {
	contentStart int
	contentEnd   int
	text         string
	globalStart  int
	globalEnd    int
}

func replaceMarkerInParagraph(paragraph, marker, value string) (string, bool) {
	matches := textPattern.FindAllStringSubmatchIndex(paragraph, -1)
	if len(matches) == 0 {
		return paragraph, false
	}
	var nodes []textNode
	var combined strings.Builder
	for _, match := range matches {
		decoded := html.UnescapeString(paragraph[match[2]:match[3]])
		start := combined.Len()
		combined.WriteString(decoded)
		nodes = append(nodes, textNode{
			contentStart: match[2], contentEnd: match[3], text: decoded,
			globalStart: start, globalEnd: combined.Len(),
		})
	}
	markerStart := strings.Index(combined.String(), marker)
	if markerStart < 0 {
		return paragraph, false
	}
	markerEnd := markerStart + len(marker)
	newTexts := make([]string, len(nodes))
	for index, node := range nodes {
		newTexts[index] = node.text
		if node.globalEnd <= markerStart || node.globalStart >= markerEnd {
			continue
		}
		left := ""
		if markerStart > node.globalStart {
			left = node.text[:markerStart-node.globalStart]
		}
		right := ""
		if markerEnd < node.globalEnd {
			right = node.text[markerEnd-node.globalStart:]
		}
		if markerStart >= node.globalStart && markerStart < node.globalEnd {
			newTexts[index] = left + value + right
		} else {
			newTexts[index] = right
		}
	}
	var result strings.Builder
	last := 0
	for index, node := range nodes {
		result.WriteString(paragraph[last:node.contentStart])
		var escaped bytes.Buffer
		_ = xml.EscapeText(&escaped, []byte(newTexts[index]))
		result.WriteString(escaped.String())
		last = node.contentEnd
	}
	result.WriteString(paragraph[last:])
	return result.String(), true
}

func paragraphText(paragraph string) string {
	matches := textPattern.FindAllStringSubmatch(paragraph, -1)
	var result strings.Builder
	for _, match := range matches {
		result.WriteString(html.UnescapeString(match[1]))
	}
	return result.String()
}

type styledRun struct {
	Text string
	Bold bool
}

func renderParagraph(template string, runs []styledRun) string {
	openEnd := strings.Index(template, ">")
	opening := "<w:p>"
	if openEnd >= 0 {
		opening = template[:openEnd+1]
	}
	properties := ""
	if match := paragraphProps.FindString(template); match != "" {
		properties = match
	}
	baseRunProperties := ""
	if match := runProps.FindStringSubmatch(template); len(match) > 1 {
		baseRunProperties = boldTag.ReplaceAllString(match[1], "")
	}
	var result strings.Builder
	result.WriteString(opening)
	result.WriteString(properties)
	for _, run := range runs {
		result.WriteString("<w:r>")
		if baseRunProperties != "" || run.Bold {
			result.WriteString("<w:rPr>")
			result.WriteString(baseRunProperties)
			if run.Bold {
				result.WriteString("<w:b/><w:bCs/>")
			}
			result.WriteString("</w:rPr>")
		}
		result.WriteString("<w:t")
		if run.Text != strings.TrimSpace(run.Text) {
			result.WriteString(` xml:space="preserve"`)
		}
		result.WriteString(">")
		var escaped bytes.Buffer
		_ = xml.EscapeText(&escaped, []byte(run.Text))
		result.WriteString(escaped.String())
		result.WriteString("</w:t></w:r>")
	}
	result.WriteString("</w:p>")
	return result.String()
}

func renderDigest(document Document) string {
	document.Edited = false
	document.EditedAt = time.Time{}
	payload, _ := json.Marshal(document)
	return digestText(string(payload))
}

func fallback(value, fallbackValue string) string {
	if strings.TrimSpace(value) == "" {
		return fallbackValue
	}
	return strings.TrimSpace(value)
}
