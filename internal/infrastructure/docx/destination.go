package docx

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"intercept/internal/domain"
)

var (
	timeLinePattern = regexp.MustCompile(`^\d{1,2}\.\d{2}\s*$`)
	badFilename     = regexp.MustCompile(`[<>:"/\\|?*]`)
)

// Destination создаёт DOCX по внешнему шаблону и доставляет его атомарно.
type Destination struct {
	TemplatePath string
	Folder       string
	StatePath    string
	Network      string
	Prefix       string

	mu sync.Mutex
}

type state struct {
	LastNumber int                    `json:"last_number"`
	Blocks     map[string]blockRecord `json:"blocks"`
}

type blockRecord struct {
	Number   int    `json:"number"`
	Hash     string `json:"hash"`
	Filename string `json:"filename"`
}

type contentBlock struct {
	Key  string
	Text string
}

func (destination *Destination) Deliver(_ context.Context, event domain.Event, _ domain.PlannedDelivery) error {
	destination.mu.Lock()
	defer destination.mu.Unlock()

	if destination.Prefix == "" {
		destination.Prefix = "СТНД"
	}
	if destination.Network == "" {
		destination.Network = "313"
	}
	if destination.TemplatePath == "" || destination.Folder == "" || destination.StatePath == "" {
		return errors.New("Word-направление настроено не полностью")
	}
	if err := os.MkdirAll(destination.Folder, 0o755); err != nil {
		return fmt.Errorf("создать папку Word: %w", err)
	}
	current, err := loadState(destination.StatePath)
	if err != nil {
		return err
	}
	for _, block := range splitBlocks(event.Content) {
		key := event.Source.UUID + "|" + block.Key
		digest := digestText(block.Text)
		record, exists := current.Blocks[key]
		if exists && record.Hash == digest {
			if _, err := os.Stat(filepath.Join(destination.Folder, record.Filename)); err == nil {
				continue
			}
		}
		if !exists {
			current.LastNumber++
			record.Number = current.LastNumber
		}
		record.Hash = digest
		record.Filename = destination.documentName(record.Number, event.Position.UnitName)
		current.Blocks[key] = record
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
		document, err := Build(destination.TemplatePath, Document{
			Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
			GroupCode: event.Position.GroupCode, Network: destination.Network,
			Date: event.ObservedAt, Operator: event.UpdatedBy, Content: block.Text,
		})
		if err != nil {
			return err
		}
		if err := writeAtomic(filepath.Join(destination.Folder, record.Filename), document); err != nil {
			return err
		}
	}
	return nil
}

// Document содержит данные одного Word-документа без привязки к файлам.
type Document struct {
	Unit      string
	Frequency string
	GroupCode string
	Network   string
	Date      time.Time
	Operator  string
	Content   string
}

// Build сохраняет все части шаблона и заменяет только тело документа.
func Build(templatePath string, document Document) ([]byte, error) {
	archive, err := zip.OpenReader(templatePath)
	if err != nil {
		return nil, fmt.Errorf("открыть шаблон Word: %w", err)
	}
	defer archive.Close()
	parts := make(map[string][]byte, len(archive.File))
	order := make([]string, 0, len(archive.File))
	for _, file := range archive.File {
		reader, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, err)
		}
		payload, readErr := io.ReadAll(reader)
		reader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, readErr)
		}
		parts[file.Name] = payload
		order = append(order, file.Name)
	}
	original, exists := parts["word/document.xml"]
	if !exists {
		return nil, errors.New("шаблон Word не содержит word/document.xml")
	}
	headIndex := bytes.Index(original, []byte("<w:body>"))
	if headIndex < 0 {
		return nil, errors.New("шаблон Word не содержит тело документа")
	}
	head := original[:headIndex]
	parts["word/document.xml"] = append(append(append([]byte{}, head...), buildBody(document)...), []byte("</w:document>")...)

	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for _, name := range order {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
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

func buildBody(document Document) []byte {
	unit := strings.TrimSpace(document.Unit)
	if unit == "" {
		unit = "н/у"
	}
	date := document.Date
	if date.IsZero() {
		date = time.Now()
	}
	var paragraphs strings.Builder
	paragraphs.WriteString(paragraph("Радиосеть "+unit+" - "+document.Network, true, "left"))
	paragraphs.WriteString(paragraph(strings.TrimSpace(document.Frequency)+" МГц (гр. ID "+fallback(document.GroupCode, "—")+" AES-256)", false, "left"))
	paragraphs.WriteString(paragraph(date.Format("02.01.2006"), true, "left"))
	paragraphs.WriteString(paragraph("", false, "left"))
	for _, line := range strings.Split(normalizeText(document.Content), "\n") {
		paragraphs.WriteString(paragraph(line, timeLinePattern.MatchString(strings.TrimSpace(line)), "left"))
	}
	operator := fallback(document.Operator, "не указан")
	body := `<w:body><w:tbl><w:tblPr><w:tblW w:w="11055" w:type="dxa"/><w:tblLayout w:type="fixed"/></w:tblPr>` +
		row(paragraph("ПЕРЕХВАТ", true, "center")) + row(paragraphs.String()) + `</w:tbl>` +
		paragraph("Дежурный оператор: "+operator, false, "right") +
		`<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="284" w:right="707" w:bottom="426" w:left="567"/></w:sectPr></w:body>`
	return []byte(body)
}

func paragraph(text string, options ...any) string {
	bold := false
	justify := "left"
	if len(options) > 0 {
		if value, ok := options[0].(bool); ok {
			bold = value
		}
	}
	if len(options) > 1 {
		if value, ok := options[1].(string); ok {
			justify = value
		}
	}
	if strings.HasPrefix(text, "<w:p>") {
		return text
	}
	var escaped bytes.Buffer
	xml.EscapeText(&escaped, []byte(text))
	boldXML := ""
	if bold {
		boldXML = "<w:b/><w:bCs/>"
	}
	return `<w:p><w:pPr><w:jc w:val="` + justify + `"/></w:pPr><w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="Times New Roman"/>` +
		boldXML + `<w:sz w:val="24"/><w:szCs w:val="24"/></w:rPr><w:t xml:space="preserve">` + escaped.String() + `</w:t></w:r></w:p>`
}

func row(paragraphs string) string {
	return `<w:tr><w:tc><w:tcPr><w:tcW w:w="11055" w:type="dxa"/><w:tcBorders>` +
		`<w:top w:val="single" w:sz="4"/><w:left w:val="single" w:sz="4"/>` +
		`<w:bottom w:val="single" w:sz="4"/><w:right w:val="single" w:sz="4"/>` +
		`</w:tcBorders></w:tcPr>` + paragraphs + `</w:tc></w:tr>`
}

func splitBlocks(content string) []contentBlock {
	lines := strings.Split(normalizeText(content), "\n")
	var starts []int
	for index, line := range lines {
		if timeLinePattern.MatchString(strings.TrimSpace(line)) {
			starts = append(starts, index)
		}
	}
	if len(starts) == 0 {
		return []contentBlock{{Key: "без времени", Text: normalizeText(content)}}
	}
	var blocks []contentBlock
	counts := map[string]int{}
	if starts[0] > 0 {
		blocks = append(blocks, contentBlock{Key: "начало", Text: strings.TrimSpace(strings.Join(lines[:starts[0]], "\n"))})
	}
	for index, start := range starts {
		end := len(lines)
		if index+1 < len(starts) {
			end = starts[index+1]
		}
		key := strings.TrimSpace(lines[start])
		counts[key]++
		if counts[key] > 1 {
			key += "#" + strconv.Itoa(counts[key])
		}
		blocks = append(blocks, contentBlock{Key: key, Text: strings.TrimSpace(strings.Join(lines[start:end], "\n"))})
	}
	result := blocks[:0]
	for _, block := range blocks {
		if block.Text != "" {
			result = append(result, block)
		}
	}
	return result
}

func loadState(path string) (state, error) {
	current := state{Blocks: make(map[string]blockRecord)}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return current, nil
	}
	if err != nil {
		return current, fmt.Errorf("прочитать состояние Word: %w", err)
	}
	if err := json.Unmarshal(payload, &current); err != nil {
		return current, errors.New("состояние Word повреждено")
	}
	if current.Blocks == nil {
		current.Blocks = make(map[string]blockRecord)
	}
	return current, nil
}

func saveState(path string, current state) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("создать папку состояния Word: %w", err)
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Word: %w", err)
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return fmt.Errorf("сохранить временное состояние Word: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("зафиксировать состояние Word: %w", err)
	}
	return nil
}

func writeAtomic(path string, payload []byte) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o644); err != nil {
		return fmt.Errorf("записать временный Word: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("доставить Word: %w", err)
	}
	return nil
}

func (destination *Destination) documentName(number int, unit string) string {
	unit = strings.Join(strings.Fields(badFilename.ReplaceAllString(unit, "")), " ")
	if unit == "" {
		unit = "н-у"
	}
	return fmt.Sprintf("%s%d %s - %s.docx", destination.Prefix, number, unit, destination.Network)
}

func normalizeText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(normalizeText(value)))
	return fmt.Sprintf("%x", digest[:])
}

func fallback(value, fallbackValue string) string {
	if strings.TrimSpace(value) == "" {
		return fallbackValue
	}
	return strings.TrimSpace(value)
}

// StateNumbers нужен диагностике и тестам без раскрытия содержимого документов.
func StateNumbers(path string) ([]int, error) {
	current, err := loadState(path)
	if err != nil {
		return nil, err
	}
	result := make([]int, 0, len(current.Blocks))
	for _, record := range current.Blocks {
		result = append(result, record.Number)
	}
	sort.Ints(result)
	return result, nil
}
