//go:build !windows

package main

import (
	"fmt"
	"os"
)

func showStartupError(message string) {
	_, _ = fmt.Fprintln(os.Stderr, message)
}
