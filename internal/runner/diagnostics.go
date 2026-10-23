package runner

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
			_, err = io.Copy(part, io.LimitReader(file, 10*1024*1024))
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
