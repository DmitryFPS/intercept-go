//go:build windows

// Package autostart registers the portable application for the current Windows user.
package autostart

import (
	"fmt"
	"strings"

	"golang.org/x/sys/windows/registry"
)

const runKey = `Software\Microsoft\Windows\CurrentVersion\Run`

// Ensure keeps autostart aligned with the folder from which the application is
// currently running. It requires no administrator rights.
func Ensure(name, executable string) error {
	name = strings.TrimSpace(name)
	if name == "" || strings.TrimSpace(executable) == "" {
		return fmt.Errorf("не заданы параметры автозапуска")
	}
	key, _, err := registry.CreateKey(registry.CURRENT_USER, runKey, registry.SET_VALUE|registry.QUERY_VALUE)
	if err != nil {
		return fmt.Errorf("открыть автозапуск Windows: %w", err)
	}
	defer key.Close()
	command := `"` + strings.ReplaceAll(executable, `"`, ``) + `" --no-browser`
	current, _, _ := key.GetStringValue(name)
	if current == command {
		return nil
	}
	if err := key.SetStringValue(name, command); err != nil {
		return fmt.Errorf("сохранить автозапуск Windows: %w", err)
	}
	return nil
}
