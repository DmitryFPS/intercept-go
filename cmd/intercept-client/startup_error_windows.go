//go:build windows

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func showStartupError(message string) {
	_, _ = fmt.Fprintln(os.Stderr, message)
	text, err := windows.UTF16PtrFromString(message)
	if err != nil {
		return
	}
	title, err := windows.UTF16PtrFromString("EAVC Connect Station — нет прав на запись")
	if err != nil {
		return
	}
	_, _ = windows.MessageBox(0, text, title, windows.MB_OK|windows.MB_ICONERROR|windows.MB_SETFOREGROUND)
}
