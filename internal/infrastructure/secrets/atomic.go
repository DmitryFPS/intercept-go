package secrets

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeAtomic never truncates the working secret. The replacement is first
// fully written and flushed in the same directory, then swapped atomically.
func writeAtomic(path string, payload []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".eavc-secret-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(payload)
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := replaceAtomic(path, temporaryPath); err != nil {
		return fmt.Errorf("атомарно заменить защищённый файл: %w", err)
	}
	return nil
}
