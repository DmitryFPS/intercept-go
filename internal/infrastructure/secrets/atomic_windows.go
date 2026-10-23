//go:build windows

package secrets

import (
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	kernel32Replace = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReplaceFileW")
)

func replaceAtomic(target, replacement string) error {
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
		return os.Rename(replacement, target)
	} else if err != nil {
		return err
	}
	targetName, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	replacementName, err := windows.UTF16PtrFromString(replacement)
	if err != nil {
		return err
	}
	result, _, callErr := kernel32Replace.Call(
		uintptr(unsafe.Pointer(targetName)), uintptr(unsafe.Pointer(replacementName)), 0, 0, 0, 0,
	)
	if result == 0 {
		return callErr
	}
	return nil
}
