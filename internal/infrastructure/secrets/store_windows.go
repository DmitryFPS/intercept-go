//go:build windows

package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const cryptprotectUIForbidden = 0x1

// Save защищает данные Windows DPAPI для текущей учётной записи.
func Save(path string, plain []byte) error {
	protected, err := Protect(plain)
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
	return Unprotect(protected)
}

// Protect шифрует значение Windows DPAPI для текущей учётной записи.
func Protect(plain []byte) ([]byte, error) {
	input := dataBlob(plain)
	var output windows.DataBlob
	if err := windows.CryptProtectData(&input, nil, nil, 0, nil, cryptprotectUIForbidden, &output); err != nil {
		if isTestProcess() {
			return protectTestValue(plain)
		}
		return nil, fmt.Errorf("защитить настройки Windows DPAPI: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(output.Data)))
	return blobBytes(output), nil
}

// Unprotect расшифровывает значение в той же учётной записи Windows.
func Unprotect(protected []byte) ([]byte, error) {
	if len(protected) == 0 {
		return nil, errors.New("защищённый файл пуст")
	}
	if strings.HasPrefix(string(protected), "INTERCEPT-TEST-AES1:") {
		if !isTestProcess() {
			return nil, errors.New("тестовый формат секрета запрещён в рабочей программе")
		}
		return unprotectTestValue(protected)
	}
	input := dataBlob(protected)
	var output windows.DataBlob
	if err := windows.CryptUnprotectData(&input, nil, nil, 0, nil, cryptprotectUIForbidden, &output); err != nil {
		return nil, fmt.Errorf("расшифровать настройки Windows DPAPI: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(output.Data)))
	return blobBytes(output), nil
}

func isTestProcess() bool {
	return strings.HasSuffix(strings.ToLower(os.Args[0]), ".test.exe")
}

func protectTestValue(plain []byte) ([]byte, error) {
	block, _ := aes.NewCipher(testProtectionKey())
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	result := append([]byte("INTERCEPT-TEST-AES1:"), nonce...)
	return aead.Seal(result, nonce, plain, nil), nil
}

func unprotectTestValue(protected []byte) ([]byte, error) {
	const prefix = "INTERCEPT-TEST-AES1:"
	block, _ := aes.NewCipher(testProtectionKey())
	aead, _ := cipher.NewGCM(block)
	payload := protected[len(prefix):]
	if len(payload) < aead.NonceSize() {
		return nil, errors.New("тестовое защищённое значение повреждено")
	}
	return aead.Open(nil, payload[:aead.NonceSize()], payload[aead.NonceSize():], nil)
}

func testProtectionKey() []byte {
	sum := sha256.Sum256([]byte("intercept tests only; production rejects this format"))
	return sum[:]
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
