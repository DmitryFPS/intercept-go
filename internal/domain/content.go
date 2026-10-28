package domain

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	// timeBlockLine принимает только целую строку заголовка. Решётка и её
	// необязательный цифровой номер являются служебной частью EAVC и никогда не
	// входят в отображаемое время. Любой иной текст после решётки оставляет
	// строку обычной репликой.
	timeBlockLine     = regexp.MustCompile(`^\s*(\d{1,2})[.:](\d{2})\s*(?:#\s*\d*)?\s*$`)
	correspondentCode = regexp.MustCompile(`\((\d{1,10})\)`)
)

// Callsign связывает цифровой ID корреспондента с подписью из EAVC.
type Callsign struct {
	Number      int64  `json:"number,omitempty"`
	Code        string `json:"code"`
	Label       string `json:"label"`
	Tag         string `json:"tag,omitempty"`
	Description string `json:"description,omitempty"`
}

// ContentBlock — одна самостоятельная временная часть бланка EAVC.
type ContentBlock struct {
	UUID      string     `json:"uuid,omitempty"`
	Key       string     `json:"key"`
	Sequence  int        `json:"sequence"`
	TimeKey   int        `json:"time_key,omitempty"`
	DayOffset int        `json:"day_offset,omitempty"`
	Text      string     `json:"text"`
	Hash      string     `json:"hash"`
	UpdatedBy string     `json:"updated_by,omitempty"`
	UpdatedAt string     `json:"updated_at,omitempty"`
	DeletedAt string     `json:"deleted_at,omitempty"`
	Callsigns []Callsign `json:"callsigns,omitempty"`
}

// Identity возвращает стабильный ключ блока. Современная EAVC выдаёт UUIDv7;
// Key остаётся совместимым запасным вариантом для архивных и старых пакетов.
func (block ContentBlock) Identity() string {
	if value := strings.TrimSpace(block.UUID); value != "" {
		return value
	}
	if value := strings.TrimSpace(block.Key); value != "" {
		return value
	}
	return "block-" + strconv.Itoa(block.Sequence+1)
}

// SplitContentBlocks разделяет каждую целую строку времени в самостоятельный
// блок. Служебные варианты 10.10, 10.10#, 10.10 #1 и 10:10 # 1 имеют один
// канонический заголовок 10:10, но каждое отдельное вхождение остаётся отдельным
// блоком. Случайная решётка в реплике сохраняется дословно.
func SplitContentBlocks(content string) []ContentBlock {
	content = normalizeBlockText(content)
	if content == "" {
		return nil
	}
	lines := strings.Split(content, "\n")
	starts := make([]int, 0)
	for index, line := range lines {
		if _, _, ok := CanonicalTimeHeader(line); ok {
			starts = append(starts, index)
		}
	}
	type part struct{ key, text string }
	parts := make([]part, 0)
	add := func(key string, chunk []string) {
		text := strings.TrimSpace(strings.Join(chunk, "\n"))
		if text != "" {
			parts = append(parts, part{key: key, text: text})
		}
	}
	if len(starts) == 0 {
		add("без времени", lines)
	} else {
		if starts[0] > 0 {
			add("начало", lines[:starts[0]])
		}
		for index, start := range starts {
			end := len(lines)
			if index+1 < len(starts) {
				end = starts[index+1]
			}
			canonical, _, _ := CanonicalTimeHeader(lines[start])
			chunk := append([]string(nil), lines[start:end]...)
			chunk[0] = canonical
			text := strings.TrimSpace(strings.Join(chunk, "\n"))
			if text != "" {
				parts = append(parts, part{key: canonical, text: text})
			}
		}
	}
	seen := make(map[string]int)
	blocks := make([]ContentBlock, 0, len(parts))
	dayOffset := 0
	previousMinutes := -1
	for sequence, item := range parts {
		seen[item.key]++
		key := item.key
		if seen[item.key] > 1 {
			key += "#" + strconv.Itoa(seen[item.key])
		}
		digest := sha256.Sum256([]byte(item.text))
		blocks = append(blocks, ContentBlock{
			Key: key, Sequence: sequence, Text: item.text, Hash: fmt.Sprintf("%x", digest[:]),
		})
		if _, minutes, ok := CanonicalTimeHeader(strings.Split(item.text, "\n")[0]); ok {
			blocks[len(blocks)-1].TimeKey = minutes
			if previousMinutes >= 0 && minutes < previousMinutes {
				dayOffset++
			}
			previousMinutes = minutes
			blocks[len(blocks)-1].DayOffset = dayOffset
		}
	}
	return blocks
}

// CanonicalTimeHeader распознаёт только полную строку заголовка и возвращает
// время HH:MM и число минут от начала суток. Пробелы, решётка и её номер не
// участвуют в результате.
func CanonicalTimeHeader(value string) (string, int, bool) {
	match := timeBlockLine.FindStringSubmatch(value)
	if match == nil {
		return "", 0, false
	}
	hour, hourErr := strconv.Atoi(match[1])
	minute, minuteErr := strconv.Atoi(match[2])
	if hourErr != nil || minuteErr != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return "", 0, false
	}
	return fmt.Sprintf("%02d:%02d", hour, minute), hour*60 + minute, true
}

// ExtractCorrespondentCodes возвращает ID в порядке первого появления.
func ExtractCorrespondentCodes(text string) []string {
	seen := make(map[string]bool)
	var codes []string
	for _, match := range correspondentCode.FindAllStringSubmatch(text, -1) {
		code := match[1]
		if !seen[code] {
			seen[code] = true
			codes = append(codes, code)
		}
	}
	return codes
}

func normalizeBlockText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}
