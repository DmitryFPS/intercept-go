package domain

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	timeBlockLine     = regexp.MustCompile(`^\d{1,2}\.\d{2}\s*$`)
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
	Text      string     `json:"text"`
	Hash      string     `json:"hash"`
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

// SplitContentBlocks повторяет разбиение рабочей Python-версии.
func SplitContentBlocks(content string) []ContentBlock {
	content = normalizeBlockText(content)
	if content == "" {
		return nil
	}
	lines := strings.Split(content, "\n")
	starts := make([]int, 0)
	for index, line := range lines {
		if timeBlockLine.MatchString(strings.TrimSpace(line)) {
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
			add(strings.TrimSpace(lines[start]), lines[start:end])
		}
	}
	seen := make(map[string]int)
	blocks := make([]ContentBlock, 0, len(parts))
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
	}
	return blocks
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
