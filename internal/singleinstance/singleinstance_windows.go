//go:build windows

// Package singleinstance prevents two workers from consuming one queue.
package singleinstance

import (
	"errors"

	"golang.org/x/sys/windows"
)

// Acquire obtains a named per-user mutex. The returned release must be called.
func Acquire(name string) (release func(), alreadyRunning bool, err error) {
	value, err := windows.UTF16PtrFromString(`Local\` + name)
	if err != nil {
		return nil, false, err
	}
	handle, createErr := windows.CreateMutex(nil, false, value)
	if createErr != nil && !errors.Is(createErr, windows.ERROR_ALREADY_EXISTS) {
		return nil, false, createErr
	}
	release = func() { _ = windows.CloseHandle(handle) }
	return release, errors.Is(createErr, windows.ERROR_ALREADY_EXISTS), nil
}
