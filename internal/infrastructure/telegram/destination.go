package telegram

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"intercept/internal/domain"
)

// Destination синхронизирует временные блоки с одной Telegram-группой.
type Destination struct {
	API       *Client
	ChatID    int64
	StatePath string

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

type destinationState struct {
	Items map[string]destinationItem `json:"items"`
}

type destinationItem struct {
	ThreadID int64                       `json:"thread_id"`
	Blocks   map[string]destinationBlock `json:"blocks"`
}

type destinationBlock struct {
	Hash       string  `json:"hash"`
	MessageIDs []int64 `json:"message_ids"`
	Sequence   int     `json:"sequence"`
}

func (destination *Destination) Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error {
	if destination.API == nil || destination.ChatID == 0 || strings.TrimSpace(destination.StatePath) == "" {
		return errors.New("Telegram-направление настроено не полностью")
	}
	itemKey := strings.TrimSpace(event.Source.UUID)
	if itemKey == "" {
		itemKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	itemLock := destination.itemLock(itemKey)
	itemLock.Lock()
	defer itemLock.Unlock()
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return err
	}
	if item.Blocks == nil {
		item.Blocks = make(map[string]destinationBlock)
	}
	threadID := delivery.Decision.ThreadID
	if item.ThreadID != 0 && item.ThreadID != threadID {
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		item.Blocks = make(map[string]destinationBlock)
	}
	item.ThreadID = threadID

	blocks := append([]domain.ContentBlock(nil), event.Blocks...)
	if blocks == nil {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	sort.SliceStable(blocks, func(left, right int) bool { return blocks[left].Sequence < blocks[right].Sequence })
	present := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		present[block.Key] = true
		chunks := formatBlockChunks(event, block, delivery)
		renderHash := hashChunks(chunks)
		stored := item.Blocks[block.Key]
		if stored.Hash == renderHash && len(stored.MessageIDs) == len(chunks) && len(chunks) > 0 {
			continue
		}
		messageIDs, err := destination.upsertBlock(ctx, threadID, stored.MessageIDs, chunks)
		if err != nil {
			return fmt.Errorf("синхронизировать блок %s: %w", block.Key, err)
		}
		item.Blocks[block.Key] = destinationBlock{
			Hash: renderHash, MessageIDs: messageIDs, Sequence: block.Sequence,
		}
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	for key, stored := range item.Blocks {
		if present[key] {
			continue
		}
		if err := destination.deleteMessages(ctx, stored.MessageIDs); err != nil {
			return fmt.Errorf("удалить исчезнувший блок %s: %w", key, err)
		}
		delete(item.Blocks, key)
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	if len(item.Blocks) == 0 {
		return destination.saveItem(itemKey, item, true)
	}
	return destination.saveItem(itemKey, item, false)
}

func (destination *Destination) itemLock(key string) *sync.Mutex {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	if destination.locks == nil {
		destination.locks = make(map[string]*sync.Mutex)
	}
	if destination.locks[key] == nil {
		destination.locks[key] = &sync.Mutex{}
	}
	return destination.locks[key]
}

func (destination *Destination) loadItem(key string) (destinationItem, error) {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		return destinationItem{}, err
	}
	return current.Items[key], nil
}

func (destination *Destination) saveItem(key string, item destinationItem, remove bool) error {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		return err
	}
	if remove {
		delete(current.Items, key)
	} else {
		current.Items[key] = item
	}
	return saveDestinationState(destination.StatePath, current)
}

func (destination *Destination) upsertBlock(
	ctx context.Context, threadID int64, oldIDs []int64, chunks []string,
) ([]int64, error) {
	ids := append([]int64(nil), oldIDs...)
	common := len(ids)
	if len(chunks) < common {
		common = len(chunks)
	}
	for index := 0; index < common; index++ {
		if err := destination.API.EditHTML(ctx, destination.ChatID, ids[index], chunks[index]); err != nil {
			if !terminalEditError(err) {
				return nil, err
			}
			message, sendErr := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
			if sendErr != nil {
				return nil, sendErr
			}
			ids[index] = message.MessageID
		}
	}
	if len(ids) > len(chunks) {
		if err := destination.deleteMessages(ctx, ids[len(chunks):]); err != nil {
			return nil, err
		}
		ids = ids[:len(chunks)]
	}
	for index := len(ids); index < len(chunks); index++ {
		message, err := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
		if err != nil {
			return nil, err
		}
		ids = append(ids, message.MessageID)
	}
	return ids, nil
}

func (destination *Destination) deleteAll(ctx context.Context, blocks map[string]destinationBlock) error {
	for _, block := range blocks {
		if err := destination.deleteMessages(ctx, block.MessageIDs); err != nil {
			return err
		}
	}
	return nil
}

func (destination *Destination) deleteMessages(ctx context.Context, ids []int64) error {
	for _, id := range ids {
		if err := destination.API.DeleteMessage(ctx, destination.ChatID, id); err != nil {
			return err
		}
	}
	return nil
}

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

func loadDestinationState(path string) (destinationState, error) {
	current := destinationState{Items: make(map[string]destinationItem)}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return current, nil
	}
	if err != nil {
		return current, fmt.Errorf("прочитать состояние Telegram: %w", err)
	}
	if err := json.Unmarshal(payload, &current); err != nil {
		return current, errors.New("состояние Telegram повреждено")
	}
	if current.Items == nil {
		current.Items = make(map[string]destinationItem)
	}
	return current, nil
}

func saveDestinationState(path string, current destinationState) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("создать папку состояния Telegram: %w", err)
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Telegram: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".telegram-state-*.tmp")
	if err != nil {
		return fmt.Errorf("создать временное состояние Telegram: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return fmt.Errorf("записать состояние Telegram: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("закрыть состояние Telegram: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("зафиксировать состояние Telegram: %w", err)
	}
	return nil
}

func terminalEditError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message can't be edited") ||
		strings.Contains(message, "message_id_invalid")
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
