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
			err = copyRedactedDiagnosticLog(part, file)
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

var diagnosticRedactedKeys = map[string]bool{
	"invite_link": true, "source_uuid": true, "source_id": true,
	"position": true, "eavc_path": true, "client_id": true,
}

// copyRedactedDiagnosticLog preserves technical events and counters, but
// removes operational identifiers from structured JSON logs before export.
// A malformed line is replaced with a marker instead of copying unknown text.
func copyRedactedDiagnosticLog(target io.Writer, source io.Reader) error {
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
			redactDiagnosticValue(record)
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

func redactDiagnosticValue(value any) {
	switch item := value.(type) {
	case map[string]any:
		for key, nested := range item {
			if diagnosticRedactedKeys[strings.ToLower(key)] {
				delete(item, key)
				continue
			}
			redactDiagnosticValue(nested)
		}
	case []any:
		for _, nested := range item {
			redactDiagnosticValue(nested)
		}
	}
}
