package runner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ClientWriteAccessError means the current Windows account was denied write
// access to a file or directory used by the portable station.
type ClientWriteAccessError struct {
	Path  string
	Cause error
}

func (err *ClientWriteAccessError) Error() string {
	return fmt.Sprintf("нет прав на запись в %s: %v", err.Path, err.Cause)
}

func (err *ClientWriteAccessError) Unwrap() error { return err.Cause }

// CheckClientWriteAccess checks real filesystem operations under the account
// running the station. A SQLite CANTOPEN error alone cannot establish an ACL
// problem: antivirus, a previous process, and temporary locks can cause it too.
// Only a concrete permission denial is returned; other storage failures retain
// the existing recovery path.
func CheckClientWriteAccess(baseDir string) *ClientWriteAccessError {
	for _, dir := range []string{baseDir, filepath.Join(baseDir, "data"), filepath.Join(baseDir, "logs")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			if accessErr := clientPermissionError(dir, err); accessErr != nil {
				return accessErr
			}
			continue
		}
		probe, err := os.CreateTemp(dir, ".eavc-write-check-*")
		if err != nil {
			if accessErr := clientPermissionError(dir, err); accessErr != nil {
				return accessErr
			}
			continue
		}
		path := probe.Name()
		closeErr := probe.Close()
		removeErr := os.Remove(path)
		if accessErr := clientPermissionError(dir, closeErr); accessErr != nil {
			return accessErr
		}
		if accessErr := clientPermissionError(dir, removeErr); accessErr != nil {
			return accessErr
		}
	}
	for _, name := range []string{
		filepath.Join(baseDir, "data", "client.db"),
		filepath.Join(baseDir, "data", "client.db-wal"),
		filepath.Join(baseDir, "data", "client.db-shm"),
		filepath.Join(baseDir, "logs", "client.log"),
	} {
		if err := checkExistingClientFileWrite(name); err != nil {
			return err
		}
	}
	return nil
}

func checkExistingClientFileWrite(path string) *ClientWriteAccessError {
	if _, err := os.Stat(path); err != nil {
		return clientPermissionError(path, err)
	}
	// Request write access without changing existing data or SQLite journal state.
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return clientPermissionError(path, err)
	}
	return clientPermissionError(path, file.Close())
}

func clientPermissionError(path string, err error) *ClientWriteAccessError {
	if err != nil && errors.Is(err, os.ErrPermission) {
		return &ClientWriteAccessError{Path: path, Cause: err}
	}
	return nil
}
