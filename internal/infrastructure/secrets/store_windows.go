//go:build windows

package secrets

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const cryptprotectUIForbidden = 0x1

// Save защищает данные Windows DPAPI для текущей учётной записи.
func Save(path string, plain []byte) error {
	protected, err := protect(plain)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, protected, 0o600); err != nil {
		return fmt.Errorf("сохранить защищённые настройки: %w", err)
	}
	return nil
}

// Load расшифровывает данные только в той же учётной записи Windows.
func Load(path string) ([]byte, error) {
	protected, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("прочитать защищённые настройки: %w", err)
	}
	return unprotect(protected)
}

func protect(plain []byte) ([]byte, error) {
	input := dataBlob(plain)
	var output windows.DataBlob
	if err := windows.CryptProtectData(&input, nil, nil, 0, nil, cryptprotectUIForbidden, &output); err != nil {
		return nil, fmt.Errorf("защитить настройки Windows DPAPI: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(output.Data)))
	return blobBytes(output), nil
}

func unprotect(protected []byte) ([]byte, error) {
	if len(protected) == 0 {
		return nil, errors.New("защищённый файл пуст")
	}
	input := dataBlob(protected)
	var output windows.DataBlob
	if err := windows.CryptUnprotectData(&input, nil, nil, 0, nil, cryptprotectUIForbidden, &output); err != nil {
		return nil, fmt.Errorf("расшифровать настройки Windows DPAPI: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(output.Data)))
	return blobBytes(output), nil
}

func dataBlob(data []byte) windows.DataBlob {
	if len(data) == 0 {
		return windows.DataBlob{}
	}
	return windows.DataBlob{Size: uint32(len(data)), Data: &data[0]}
}

func blobBytes(blob windows.DataBlob) []byte {
	if blob.Size == 0 || blob.Data == nil {
		return nil
	}
	return append([]byte(nil), unsafe.Slice(blob.Data, blob.Size)...)
}
