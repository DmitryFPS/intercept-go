package docx

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"intercept/internal/domain"
)

var (
	legacyDatePrefix = regexp.MustCompile(`^\s*(\d{4})-(\d{2})-(\d{2})`)
)

const (
	directionTitle     = "ДНЕПРОПЕТРОВСКОЕ НАПРАВЛЕНИЕ"
	defaultNetwork     = "313"
	defaultOperator    = "Позывной"
	defaultShiftGroup  = "1"
	defaultShiftChief  = "Позывной"
	editedMark         = "(отредактировано)"
	cellRunProperties  = `<w:rFonts w:eastAsia="Times New Roman"/>{b}<w:szCs w:val="24"/><w:lang w:eastAsia="ru-RU"/>`
	titleRunProperties = `<w:rFonts w:eastAsia="Times New Roman"/>{b}<w:bCs/><w:szCs w:val="24"/><w:lang w:eastAsia="ru-RU"/>`
	outerRunProperties = `<w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:cs="Times New Roman"/><w:sz w:val="28"/>`

	maxTemplateParts      = 2048
	maxTemplatePartBytes  = 16 * 1024 * 1024
	maxTemplateTotalBytes = 64 * 1024 * 1024
)

// Document содержит данные одного Word-документа без привязки к файлам.
type Document struct {
	Unit            string
	Frequency       string
	GroupCode       string
	Network         string
	Date            time.Time
	Operator        string
	Content         string
	Callsigns       []domain.Callsign
	SourceUpdatedAt string
	Edited          bool
	EditedAt        time.Time
}

// Build сохраняет служебные части шаблона и заменяет только тело документа.
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
	parts["word/document.xml"] = append(append(append([]byte{}, original[:headIndex]...), buildBody(document)...), []byte("</w:document>")...)

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
	correspondents, lines := domain.PresentIntercept(document.Content, document.Callsigns)
	var paragraphs strings.Builder
	paragraphs.WriteString(styledParagraph([]styledRun{
		{Text: "Радиосеть "}, {Text: unit, Bold: true}, {Text: " - " + network},
	}, true, ""))
	paragraphs.WriteString(styledParagraph([]styledRun{{
		Text: fallback(document.Frequency, "—") + " МГц (гр. ID " + fallback(document.GroupCode, "—") + " AES-256)",
	}}, false, ""))
	dateText := date.Format("02.01.2006г.")
	if match := legacyDatePrefix.FindStringSubmatch(document.SourceUpdatedAt); match != nil {
		dateText = match[3] + "." + match[2] + "." + match[1] + "г."
	}
	paragraphs.WriteString(styledParagraph([]styledRun{{Text: dateText, Bold: true}}, true, ""))
	paragraphs.WriteString(styledParagraph(nil, false, ""))
	for _, correspondent := range correspondents {
		description := strings.TrimSpace(correspondent.Description)
		if description == "" {
			description = "военнослужащий " + unit + ", мужской голос, украинская речь"
		}
		description = strings.TrimRight(description, ";") + ";"
		paragraphs.WriteString(styledParagraph([]styledRun{
			{Text: strconv.Itoa(correspondent.Number) + ".«"},
			{Text: correspondent.Label, Bold: true},
			{Text: "» (id: " + correspondent.Code + ") – " + description},
		}, false, ""))
	}
	if len(correspondents) > 0 {
		paragraphs.WriteString(styledParagraph(nil, false, ""))
	}
	for _, line := range lines {
		paragraphs.WriteString(styledParagraph([]styledRun{{Text: line.Text, Bold: line.Time}}, line.Time, ""))
	}
	if document.Edited {
		paragraphs.WriteString(styledParagraph(nil, false, ""))
		paragraphs.WriteString(styledParagraph([]styledRun{{Text: editedMark}}, false, ""))
	}
	body := `<w:body>` + styledParagraphWith(nil, false, "", outerRunProperties, 480) +
		`<w:tbl><w:tblPr><w:tblStyle w:val="a3"/><w:tblW w:w="11055" w:type="dxa"/><w:tblInd w:w="-34" w:type="dxa"/><w:tblLayout w:type="fixed"/><w:tblLook w:val="04A0" w:firstRow="1" w:lastRow="0" w:firstColumn="1" w:lastColumn="0" w:noHBand="0" w:noVBand="1"/></w:tblPr><w:tblGrid><w:gridCol w:w="11055"/></w:tblGrid>` +
		styledRow(styledParagraphWith([]styledRun{{Text: directionTitle, Bold: true, Properties: titleRunProperties}}, false, "center", titleRunProperties, -1), true) +
		styledRow(paragraphs.String(), false) + `</w:tbl>` +
		styledParagraphWith([]styledRun{{Text: "Дежурный оператор: ", Properties: outerRunProperties}, {Text: defaultOperator, Properties: outerRunProperties}}, false, "right", outerRunProperties, -1) +
		styledParagraphWith([]styledRun{{Text: "Старший смены МГ ", Properties: outerRunProperties}, {Text: defaultShiftGroup, Properties: outerRunProperties}, {Text: ": ", Properties: outerRunProperties}, {Text: defaultShiftChief, Properties: outerRunProperties}}, false, "center", outerRunProperties, -1) +
		styledParagraphWith(nil, false, "", outerRunProperties, -1) +
		`<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="284" w:right="707" w:bottom="426" w:left="567" w:header="708" w:footer="708" w:gutter="0"/><w:cols w:space="708"/><w:docGrid w:linePitch="360"/></w:sectPr></w:body>`
	return []byte(body)
}

type styledRun struct {
	Text       string
	Bold       bool
	Properties string
}

func styledParagraph(runs []styledRun, boldMark bool, justify string) string {
	return styledParagraphWith(runs, boldMark, justify, cellRunProperties, -1)
}

func styledParagraphWith(runs []styledRun, boldMark bool, justify, properties string, spacingAfter int) string {
	var builder strings.Builder
	builder.WriteString(`<w:p><w:pPr>`)
	if spacingAfter >= 0 {
		builder.WriteString(`<w:spacing w:after="` + strconv.Itoa(spacingAfter) + `"/>`)
	}
	if justify != "" {
		builder.WriteString(`<w:jc w:val="` + justify + `"/>`)
	}
	builder.WriteString(`<w:rPr>`)
	builder.WriteString(formatRunProperties(properties, boldMark))
	builder.WriteString(`</w:rPr></w:pPr>`)
	for _, run := range runs {
		var escaped bytes.Buffer
		_ = xml.EscapeText(&escaped, []byte(run.Text))
		runProperties := run.Properties
		if runProperties == "" {
			runProperties = cellRunProperties
		}
		builder.WriteString(`<w:r><w:rPr>`)
		builder.WriteString(formatRunProperties(runProperties, run.Bold))
		builder.WriteString(`</w:rPr><w:t`)
		if run.Text != strings.TrimSpace(run.Text) {
			builder.WriteString(` xml:space="preserve"`)
		}
		builder.WriteString(`>`)
		builder.WriteString(escaped.String())
		builder.WriteString(`</w:t></w:r>`)
	}
	builder.WriteString(`</w:p>`)
	return builder.String()
}

func formatRunProperties(properties string, bold bool) string {
	marker := ""
	if bold {
		marker = `<w:b/>`
	}
	return strings.Replace(properties, "{b}", marker, 1)
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
