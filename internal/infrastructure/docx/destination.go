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
	"intercept/internal/infrastructure/share"
)

var (
	timeLinePattern = regexp.MustCompile(`^(\d{1,2})[.:](\d{2})\s*$`)
	badFilename     = regexp.MustCompile(`[<>:"/\\|?*]`)
	codeTailPattern = regexp.MustCompile(`\s*\((\d{1,10})\)\s*$`)
	codeAnyPattern  = regexp.MustCompile(`\((\d{1,10})\)`)
	leadingDash     = regexp.MustCompile(`^[-–—]\s*`)
)

const (
	directionTitle    = "ДНЕПРОПЕТРОВСКОЕ НАПРАВЛЕНИЕ"
	defaultNetwork    = "313"
	defaultOperator   = "Позывной"
	defaultShiftGroup = "1"
	defaultShiftChief = "Позывной"
	editedMark        = "(отредактировано)"
)

var errDestinationBusy = errors.New("предыдущая операция Word ещё выполняется")

// Destination создаёт DOCX по внешнему шаблону и доставляет его атомарно.
type Destination struct {
	TemplatePath string
	Folder       string
	StatePath    string
	Network      string
	Prefix       string
	Username     string
	Password     string
}

type destinationLock struct {
	mutex sync.Mutex
	refs  int
}

var destinationLocks = struct {
	sync.Mutex
	items map[string]*destinationLock
}{items: make(map[string]*destinationLock)}

type state struct {
	LastNumber int                    `json:"last_number"`
	Blocks     map[string]blockRecord `json:"blocks"`
}

type blockRecord struct {
	Number    int       `json:"number"`
	Hash      string    `json:"hash"`
	Render    string    `json:"render,omitempty"`
	Filename  string    `json:"filename"`
	Edited    bool      `json:"edited,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (destination *Destination) Deliver(ctx context.Context, event domain.Event, _ domain.PlannedDelivery) error {
	settings := *destination
	if settings.Prefix == "" {
		settings.Prefix = "СТНД"
	}
	if settings.Network == "" {
		settings.Network = defaultNetwork
	}
	if settings.TemplatePath == "" || settings.Folder == "" || settings.StatePath == "" {
		return errors.New("Word-направление настроено не полностью")
	}
	unlock, acquired := tryDestinationLock(settings.StatePath)
	if !acquired {
		return errDestinationBusy
	}
	result := make(chan error, 1)
	go func() {
		defer unlock()
		result <- settings.deliver(ctx, event)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("операция Word превысила допустимое время: %w", ctx.Err())
	}
}

func (destination Destination) deliver(ctx context.Context, event domain.Event) error {
	if _, err := os.Stat(destination.Folder); err != nil {
		if err := share.Connect(ctx, destination.Folder, destination.Username, destination.Password); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(destination.Folder, 0o755); err != nil {
		return fmt.Errorf("создать папку Word: %w", err)
	}
	current, err := loadState(destination.StatePath)
	if err != nil {
		return err
	}
	blocks := event.Blocks
	if blocks == nil {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	sourceKey := strings.TrimSpace(event.Source.UUID)
	if sourceKey == "" {
		sourceKey = "id:" + strconv.FormatInt(event.Source.ID, 10)
	}
	prefix := sourceKey + "|"
	present := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		key := prefix + block.Key
		present[key] = true
		digest := digestText(block.Text)
		record, exists := current.Blocks[key]
		if !exists {
			current.LastNumber++
			record.Number = current.LastNumber
		}
		date := event.SourceAt
		if date.IsZero() {
			date = event.ObservedAt
		}
		document := Document{
			Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
			GroupCode: event.Position.GroupCode, Network: destination.Network,
			Date: date, Operator: event.UpdatedBy, Content: block.Text,
			Callsigns: block.Callsigns,
		}
		render := renderDigest(document)
		filename := destination.documentName(record.Number, event.Position.UnitName)
		if exists && ((record.Render != "" && record.Render == render) ||
			(record.Render == "" && record.Hash == digest && record.Filename == filename)) {
			continue
		}
		document.Edited = record.Edited || (exists && record.Render != "" && record.Render != render)
		payload, err := Build(destination.TemplatePath, document)
		if err != nil {
			return err
		}
		if err := writeAtomic(filepath.Join(destination.Folder, filename), payload); err != nil {
			return err
		}
		if record.Filename != "" && record.Filename != filename {
			_ = os.Remove(filepath.Join(destination.Folder, record.Filename))
		}
		record.Hash = digest
		record.Render = render
		record.Filename = filename
		record.Edited = document.Edited
		record.UpdatedAt = time.Now().UTC()
		current.Blocks[key] = record
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
	}
	for key, record := range current.Blocks {
		if !strings.HasPrefix(key, prefix) || present[key] {
			continue
		}
		if record.Filename != "" {
			_ = os.Remove(filepath.Join(destination.Folder, record.Filename))
		}
		delete(current.Blocks, key)
	}
	if err := saveState(destination.StatePath, current); err != nil {
		return err
	}
	return nil
}

func tryDestinationLock(path string) (func(), bool) {
	key, err := filepath.Abs(path)
	if err != nil {
		key = filepath.Clean(path)
	}
	key = strings.ToLower(filepath.Clean(key))
	destinationLocks.Lock()
	entry := destinationLocks.items[key]
	if entry == nil {
		entry = &destinationLock{}
		destinationLocks.items[key] = entry
	}
	entry.refs++
	destinationLocks.Unlock()
	if !entry.mutex.TryLock() {
		releaseDestinationLock(key, entry, false)
		return nil, false
	}
	return func() { releaseDestinationLock(key, entry, true) }, true
}

func releaseDestinationLock(key string, entry *destinationLock, held bool) {
	if held {
		entry.mutex.Unlock()
	}
	destinationLocks.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(destinationLocks.items, key)
	}
	destinationLocks.Unlock()
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
	Callsigns []domain.Callsign
	Edited    bool
}

const (
	maxTemplateParts      = 2048
	maxTemplatePartBytes  = 16 * 1024 * 1024
	maxTemplateTotalBytes = 64 * 1024 * 1024
)

// Build сохраняет все части шаблона и заменяет только тело документа.
func Build(templatePath string, document Document) ([]byte, error) {
	archive, err := zip.OpenReader(templatePath)
	if err != nil {
		return nil, fmt.Errorf("открыть шаблон Word: %w", err)
	}
	defer archive.Close()
	if len(archive.File) > maxTemplateParts {
		return nil, fmt.Errorf("шаблон Word содержит слишком много частей: %d", len(archive.File))
	}
	parts := make(map[string][]byte, len(archive.File))
	order := make([]string, 0, len(archive.File))
	var totalSize uint64
	for _, file := range archive.File {
		if file.UncompressedSize64 > maxTemplatePartBytes ||
			totalSize+file.UncompressedSize64 > maxTemplateTotalBytes {
			return nil, fmt.Errorf("шаблон Word слишком велик после распаковки: %s", file.Name)
		}
		if _, duplicate := parts[file.Name]; duplicate {
			return nil, fmt.Errorf("шаблон Word содержит повторяющуюся часть: %s", file.Name)
		}
		reader, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(reader, maxTemplatePartBytes+1))
		reader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("прочитать часть шаблона %s: %w", file.Name, readErr)
		}
		if len(payload) > maxTemplatePartBytes {
			return nil, fmt.Errorf("часть шаблона Word слишком велика: %s", file.Name)
		}
		totalSize += uint64(len(payload))
		if totalSize > maxTemplateTotalBytes {
			return nil, errors.New("шаблон Word слишком велик после распаковки")
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
	network := strings.TrimSpace(document.Network)
	if network == "" {
		network = defaultNetwork
	}
	date := document.Date
	if date.IsZero() {
		date = time.Now()
	}
	date = date.In(time.FixedZone("MSK", 3*60*60))
	correspondents, lines := parseDocumentContent(document.Content, document.Callsigns)
	var paragraphs strings.Builder
	paragraphs.WriteString(styledParagraph([]styledRun{
		{Text: "Радиосеть "}, {Text: unit, Bold: true}, {Text: " - " + network},
	}, false, "left"))
	paragraphs.WriteString(styledParagraph([]styledRun{{
		Text: strings.TrimSpace(document.Frequency) + " МГц (гр. ID " + fallback(document.GroupCode, "—") + " AES-256)",
	}}, false, "left"))
	paragraphs.WriteString(styledParagraph([]styledRun{{Text: date.Format("02.01.2006г."), Bold: true}}, true, "left"))
	paragraphs.WriteString(styledParagraph(nil, false, "left"))
	for _, correspondent := range correspondents {
		paragraphs.WriteString(styledParagraph([]styledRun{
			{Text: strconv.Itoa(correspondent.Number) + ".«"},
			{Text: correspondent.Label, Bold: true},
			{Text: "» (id: " + correspondent.Code + ") – военнослужащий " + unit + ", мужской голос, украинская речь;"},
		}, false, "left"))
	}
	if len(correspondents) > 0 {
		paragraphs.WriteString(styledParagraph(nil, false, "left"))
	}
	for _, line := range lines {
		paragraphs.WriteString(styledParagraph([]styledRun{{Text: line.Text, Bold: line.Time}}, line.Time, "left"))
	}
	if document.Edited {
		paragraphs.WriteString(styledParagraph(nil, false, "left"))
		paragraphs.WriteString(styledParagraph([]styledRun{{Text: editedMark}}, false, "left"))
	}
	body := `<w:body>` + styledParagraph(nil, false, "left") +
		`<w:tbl><w:tblPr><w:tblStyle w:val="a3"/><w:tblW w:w="11055" w:type="dxa"/><w:tblInd w:w="-34" w:type="dxa"/><w:tblLayout w:type="fixed"/><w:tblLook w:val="04A0" w:firstRow="1" w:lastRow="0" w:firstColumn="1" w:lastColumn="0" w:noHBand="0" w:noVBand="1"/></w:tblPr><w:tblGrid><w:gridCol w:w="11055"/></w:tblGrid>` +
		styledRow(styledParagraph([]styledRun{{Text: directionTitle, Bold: true}}, false, "center"), true) +
		styledRow(paragraphs.String(), false) + `</w:tbl>` +
		styledParagraph([]styledRun{{Text: "Дежурный оператор: "}, {Text: defaultOperator}}, false, "right") +
		styledParagraph([]styledRun{{Text: "Старший смены МГ "}, {Text: defaultShiftGroup}, {Text: ": "}, {Text: defaultShiftChief}}, false, "center") +
		styledParagraph(nil, false, "left") +
		`<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="284" w:right="707" w:bottom="426" w:left="567" w:header="708" w:footer="708" w:gutter="0"/><w:cols w:space="708"/><w:docGrid w:linePitch="360"/></w:sectPr></w:body>`
	return []byte(body)
}

type correspondent struct {
	Number int
	Label  string
	Code   string
}

type documentLine struct {
	Text string
	Time bool
}

type styledRun struct {
	Text string
	Bold bool
}

func parseDocumentContent(content string, callsigns []domain.Callsign) ([]correspondent, []documentLine) {
	labels := make(map[string]string)
	for _, callsign := range callsigns {
		code := normalizeCode(callsign.Code)
		label := strings.TrimSpace(callsign.Label)
		if code != "" && label != "" && label != "н/у" && labels[code] == "" {
			labels[code] = label
		}
	}
	codes := domain.ExtractCorrespondentCodes(content)
	numbers := make(map[string]int, len(codes))
	correspondents := make([]correspondent, 0, len(codes))
	for _, code := range codes {
		numbers[code] = len(numbers) + 1
		label := labels[normalizeCode(code)]
		if label == "" {
			label = "н/у"
		}
		correspondents = append(correspondents, correspondent{Number: numbers[code], Label: label, Code: code})
	}
	lines := make([]documentLine, 0)
	for _, raw := range strings.Split(normalizeText(content), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if match := timeLinePattern.FindStringSubmatch(line); match != nil {
			hour, _ := strconv.Atoi(match[1])
			lines = append(lines, documentLine{Text: fmt.Sprintf("%02d:%s", hour, match[2]), Time: true})
			continue
		}
		body := leadingDash.ReplaceAllString(line, "")
		code := ""
		if match := codeTailPattern.FindStringSubmatchIndex(body); match != nil {
			code = body[match[2]:match[3]]
			body = strings.TrimSpace(body[:match[0]])
		} else if match := codeAnyPattern.FindStringSubmatchIndex(body); match != nil {
			code = body[match[2]:match[3]]
			body = strings.TrimSpace(body[:match[0]] + body[match[1]:])
		}
		if number := numbers[code]; number > 0 {
			body = "/" + strconv.Itoa(number) + "/ " + body
		}
		lines = append(lines, documentLine{Text: strings.TrimSpace(body)})
	}
	return correspondents, lines
}

func normalizeCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.TrimLeft(value, "0")
	if value == "" {
		return "0"
	}
	return value
}

func styledParagraph(runs []styledRun, boldMark bool, justify string) string {
	var builder strings.Builder
	builder.WriteString(`<w:p><w:pPr><w:jc w:val="` + justify + `"/><w:rPr><w:rFonts w:eastAsia="Times New Roman"/>`)
	if boldMark {
		builder.WriteString(`<w:b/>`)
	}
	builder.WriteString(`<w:szCs w:val="24"/><w:lang w:eastAsia="ru-RU"/></w:rPr></w:pPr>`)
	for _, run := range runs {
		var escaped bytes.Buffer
		_ = xml.EscapeText(&escaped, []byte(run.Text))
		builder.WriteString(`<w:r><w:rPr><w:rFonts w:eastAsia="Times New Roman"/>`)
		if run.Bold {
			builder.WriteString(`<w:b/>`)
		}
		builder.WriteString(`<w:szCs w:val="24"/><w:lang w:eastAsia="ru-RU"/></w:rPr><w:t xml:space="preserve">`)
		builder.WriteString(escaped.String())
		builder.WriteString(`</w:t></w:r>`)
	}
	builder.WriteString(`</w:p>`)
	return builder.String()
}

func styledRow(paragraphs string, verticalCenter bool) string {
	vertical := ""
	if verticalCenter {
		vertical = `<w:vAlign w:val="center"/><w:hideMark/>`
	}
	return `<w:tr><w:trPr><w:trHeight w:val="389"/></w:trPr><w:tc><w:tcPr><w:tcW w:w="11055" w:type="dxa"/><w:tcBorders><w:top w:val="single" w:sz="4" w:space="0" w:color="auto"/><w:left w:val="single" w:sz="4" w:space="0" w:color="auto"/><w:bottom w:val="single" w:sz="4" w:space="0" w:color="auto"/><w:right w:val="single" w:sz="4" w:space="0" w:color="auto"/></w:tcBorders>` + vertical + `</w:tcPr>` + paragraphs + `</w:tc></w:tr>`
}

func renderDigest(document Document) string {
	document.Edited = false
	payload, _ := json.Marshal(document)
	return digestText(string(payload))
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
	legacyTime := time.Now().UTC()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyTime = info.ModTime().UTC()
	}
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for key, record := range current.Blocks {
		if record.UpdatedAt.IsZero() {
			record.UpdatedAt = legacyTime
			current.Blocks[key] = record
		}
		if record.UpdatedAt.Before(cutoff) {
			delete(current.Blocks, key)
		}
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
