package runner

import (
	"archive/zip"
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"intercept/internal/observability"
)

// addDiagnosticLogs includes the complete bounded log set. Read failures are
// returned instead of silently producing an incomplete archive.
func addDiagnosticLogs(archive *zip.Writer, baseDir, primary string) error {
	redactor, err := newDiagnosticRedactor()
	if err != nil {
		return err
	}
	paths := make([]string, 0, 24)
	for index := 0; index <= 5; index++ {
		name := primary
		if index > 0 {
			name = fmt.Sprintf("%s.%d", primary, index)
		}
		path := filepath.Join(baseDir, "logs", name)
		if _, err := os.Stat(path); err == nil {
			paths = append(paths, path)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	paths = append(paths, observability.OperationalFiles(baseDir)...)
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		clean := filepath.Clean(path)
		if seen[clean] {
			continue
		}
		seen[clean] = true
		file, err := os.Open(clean)
		if err != nil {
			return err
		}
		part, err := archive.Create(filepath.Join("logs", filepath.Base(clean)))
		if err == nil {
			switch filepath.Base(clean) {
			case "latest-update.json":
				err = copyRedactedDiagnosticJSONWithRedactor(part, file, true, redactor)
			case "updates.log":
				err = copyRedactedDiagnosticLogWithRedactor(part, file, true, redactor)
			default:
				err = copyRedactedDiagnosticLogWithRedactor(part, file, false, redactor)
			}
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

var diagnosticAllowedKeys = map[string]bool{
	"time": true, "level": true, "msg": true, "event": true, "component": true,
	"version": true, "from": true, "to": true, "updated_at": true,
	"uptime_seconds": true, "memory_bytes": true, "goroutines": true, "disk_free_bytes": true,
	"status": true, "state": true, "configured": true, "transport_ok": true,
	"database_ok": true, "telegram_ok": true, "processing": true,
	"queue": true, "stats": true, "pending": true, "uploaded": true,
	"acknowledged": true, "delivered": true, "expired": true, "rejected": true,
	"clients": true, "clients_online": true, "clients_degraded": true,
	"clients_offline": true, "events": true, "attempt": true, "attempts": true,
	"transport_inbox_pending": true, "packets": true, "next_transport_offset": true,
	"last_received": true, "last_delivery": true, "last_database_read": true,
	"last_upload": true, "last_acknowledged": true, "last_transport_check": true,
	"next_attempt_at": true, "accepted_at": true, "expires_at": true,
	"from_version": true, "to_version": true,
	"content_kind": true, "content_chars": true, "content_blocks": true,
	"source_mode": true, "new_event": true, "telegram_message_count": true,
	"removed":  true,
	"trace_id": true, "transport_message_id": true, "transport_update_id": true,
	"downloaded_at": true, "outcome": true,
	"counted": true, "transport": true, "final_message_id": true, "final_message_ids": true,
	"event_ref": true, "source_ref": true, "source_id_ref": true, "item_ref": true,
	"client_ref": true, "destination_ref": true, "chat_ref": true, "thread_ref": true,
	"message_ref": true, "telegram_message_ref": true,
	"sender_bot_ref": true, "sender_bot_id_ref": true,
	"error": true,
}

var diagnosticReferenceKeys = map[string]string{
	"event_id": "event_ref", "source_uuid": "source_ref", "source_id": "source_id_ref",
	"item_key": "item_ref", "client_id": "client_ref", "destination_id": "destination_ref",
	"chat_id": "chat_ref", "thread_id": "thread_ref", "message_id": "message_ref",
	"telegram_message_id": "telegram_message_ref", "sender_bot": "sender_bot_ref",
	"sender_bot_id": "sender_bot_id_ref",
}

type diagnosticRedactor struct{ key [32]byte }

func newDiagnosticRedactor() (*diagnosticRedactor, error) {
	redactor := &diagnosticRedactor{}
	if _, err := rand.Read(redactor.key[:]); err != nil {
		return nil, fmt.Errorf("создать ключ обезличивания диагностики: %w", err)
	}
	return redactor, nil
}

func (redactor *diagnosticRedactor) reference(kind string, value any) string {
	mac := hmac.New(sha256.New, redactor.key[:])
	_, _ = fmt.Fprintf(mac, "%s\x00%v", kind, value)
	return fmt.Sprintf("%s-%x", strings.TrimSuffix(kind, "_ref"), mac.Sum(nil)[:8])
}

// copyRedactedDiagnosticLog preserves technical events and counters, but
// removes operational identifiers from structured JSON logs before export.
// A malformed line is replaced with a marker instead of copying unknown text.
func copyRedactedDiagnosticLog(target io.Writer, source io.Reader) error {
	return copyRedactedDiagnosticLogWithUpdates(target, source, false)
}

func copyRedactedDiagnosticLogWithUpdates(target io.Writer, source io.Reader, updateDetails bool) error {
	redactor, err := newDiagnosticRedactor()
	if err != nil {
		return err
	}
	return copyRedactedDiagnosticLogWithRedactor(target, source, updateDetails, redactor)
}

func copyRedactedDiagnosticLogWithRedactor(
	target io.Writer, source io.Reader, updateDetails bool, redactor *diagnosticRedactor,
) error {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	var copied int64
	for scanner.Scan() {
		copied += int64(len(scanner.Bytes())) + 1
		if copied > 10*1024*1024 {
			break
		}
		var record any
		if json.Unmarshal(scanner.Bytes(), &record) != nil {
			record = map[string]any{"event": "строка журнала скрыта: неизвестный формат"}
		} else {
			allowDiagnosticValue(record, updateDetails, redactor)
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := target.Write(append(payload, '\n')); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func copyRedactedDiagnosticJSON(target io.Writer, source io.Reader, updateDetails bool) error {
	redactor, err := newDiagnosticRedactor()
	if err != nil {
		return err
	}
	return copyRedactedDiagnosticJSONWithRedactor(target, source, updateDetails, redactor)
}

func copyRedactedDiagnosticJSONWithRedactor(
	target io.Writer, source io.Reader, updateDetails bool, redactor *diagnosticRedactor,
) error {
	payload, err := io.ReadAll(io.LimitReader(source, 10*1024*1024))
	if err != nil {
		return err
	}
	var record any
	if err := json.Unmarshal(payload, &record); err != nil {
		record = map[string]any{"event": "документ диагностики скрыт: неизвестный формат"}
	} else {
		allowDiagnosticValue(record, updateDetails, redactor)
	}
	clean, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	_, err = target.Write(append(clean, '\n'))
	return err
}

func allowDiagnosticValue(value any, updateDetails bool, redactor *diagnosticRedactor) {
	switch item := value.(type) {
	case map[string]any:
		for key, nested := range item {
			lower := strings.ToLower(key)
			if referenceKey, exists := diagnosticReferenceKeys[lower]; exists {
				if nested != nil && fmt.Sprint(nested) != "" {
					item[referenceKey] = redactor.reference(referenceKey, nested)
				}
				delete(item, key)
				continue
			}
			allowed := diagnosticAllowedKeys[lower] || (updateDetails && (lower == "message" || lower == "error"))
			if !allowed {
				delete(item, key)
				continue
			}
			if text, ok := nested.(string); ok {
				if lower == "error" {
					item[key] = safeDiagnosticError(text)
				} else {
					item[key] = observability.SafeText(text)
				}
				continue
			}
			allowDiagnosticValue(nested, updateDetails, redactor)
		}
	case []any:
		for index, nested := range item {
			if text, ok := nested.(string); ok {
				item[index] = observability.SafeText(text)
				continue
			}
			allowDiagnosticValue(nested, updateDetails, redactor)
		}
	}
}

func safeDiagnosticError(value string) string {
	fields := strings.Fields(observability.SafeText(value))
	for index, field := range fields {
		if strings.Contains(field, `:\`) || strings.HasPrefix(field, `\\`) {
			fields[index] = "[PATH]"
		}
	}
	return strings.Join(fields, " ")
}
