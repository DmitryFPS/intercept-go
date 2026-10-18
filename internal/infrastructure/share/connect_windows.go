//go:build windows

package share

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Connect подключает только корень UNC-шары; пароль передаётся через stdin.
func Connect(ctx context.Context, path, username, password string) error {
	root := uncRoot(path)
	if root == "" || strings.TrimSpace(username) == "" {
		return nil
	}
	timed, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	command := exec.CommandContext(timed, "net", "use", root, "*", "/user:"+username, "/persistent:no")
	command.Stdin = strings.NewReader(password + "\r\n")
	output, err := command.CombinedOutput()
	if timed.Err() != nil {
		return errors.New("подключение к сетевой папке превысило 20 секунд")
	}
	if err != nil {
		message := strings.TrimSpace(string(bytes.ReplaceAll(output, []byte(password), []byte("***"))))
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("не удалось подключить сетевую папку %s: %s", root, message)
	}
	return nil
}

func uncRoot(path string) string {
	path = strings.ReplaceAll(strings.TrimSpace(path), "/", "\\")
	if !strings.HasPrefix(path, `\\`) {
		return ""
	}
	parts := strings.FieldsFunc(strings.TrimPrefix(path, `\\`), func(char rune) bool { return char == '\\' })
	if len(parts) < 2 {
		return ""
	}
	return `\\` + parts[0] + `\` + parts[1]
}
