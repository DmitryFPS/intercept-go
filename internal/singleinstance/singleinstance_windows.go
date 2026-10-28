//go:build windows

// Package singleinstance prevents two workers from consuming one queue.
package singleinstance

import (
	"errors"

	"golang.org/x/sys/windows"
)

// Acquire obtains one mutex for the whole Windows machine. The Global namespace
// is essential here: autostart, VMware tools and an interactive user can run in
// different Windows sessions while still pointing at the same SQLite queue.
// A session-local mutex allowed both processes to consume and migrate that queue.
func Acquire(name string) (release func(), alreadyRunning bool, err error) {
	value, err := windows.UTF16PtrFromString(globalMutexName(name))
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

func globalMutexName(name string) string {
	return `Global\` + name
}
