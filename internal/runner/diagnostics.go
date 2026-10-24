package runner

import (
	"archive/zip"
	"bufio"
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
				err = copyRedactedDiagnosticJSON(part, file, true)
			case "updates.log":
				err = copyRedactedDiagnosticLogWithUpdates(part, file, true)
			default:
				err = copyRedactedDiagnosticLog(part, file)
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
	"last_received": true, "last_delivery": true, "last_database_read": true,
	"last_upload": true, "last_acknowledged": true, "last_transport_check": true,
	"next_attempt_at": true, "accepted_at": true, "expires_at": true,
	"from_version": true, "to_version": true,
}

// copyRedactedDiagnosticLog preserves technical events and counters, but
// removes operational identifiers from structured JSON logs before export.
// A malformed line is replaced with a marker instead of copying unknown text.
func copyRedactedDiagnosticLog(target io.Writer, source io.Reader) error {
	return copyRedactedDiagnosticLogWithUpdates(target, source, false)
}

func copyRedactedDiagnosticLogWithUpdates(target io.Writer, source io.Reader, updateDetails bool) error {
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
			allowDiagnosticValue(record, updateDetails)
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
	payload, err := io.ReadAll(io.LimitReader(source, 10*1024*1024))
	if err != nil {
		return err
	}
	var record any
	if err := json.Unmarshal(payload, &record); err != nil {
		record = map[string]any{"event": "документ диагностики скрыт: неизвестный формат"}
	} else {
		allowDiagnosticValue(record, updateDetails)
	}
	clean, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	_, err = target.Write(append(clean, '\n'))
	return err
}

func allowDiagnosticValue(value any, updateDetails bool) {
	switch item := value.(type) {
	case map[string]any:
		for key, nested := range item {
			lower := strings.ToLower(key)
			allowed := diagnosticAllowedKeys[lower] || (updateDetails && (lower == "message" || lower == "error"))
			if !allowed {
				delete(item, key)
				continue
			}
			if text, ok := nested.(string); ok {
				item[key] = observability.SafeText(text)
				continue
			}
			allowDiagnosticValue(nested, updateDetails)
		}
	case []any:
		for index, nested := range item {
			if text, ok := nested.(string); ok {
				item[index] = observability.SafeText(text)
				continue
			}
			allowDiagnosticValue(nested, updateDetails)
		}
	}
}
