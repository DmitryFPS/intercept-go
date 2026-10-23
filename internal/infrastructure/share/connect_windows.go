//go:build windows

package share

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	resourceTypeDisk               = 1
	errorSessionCredentialConflict = windows.Errno(1219)
)

type netResource struct {
	Scope       uint32
	Type        uint32
	DisplayType uint32
	Usage       uint32
	LocalName   *uint16
	RemoteName  *uint16
	Comment     *uint16
	Provider    *uint16
}

var (
	mprDLL              = windows.NewLazySystemDLL("mpr.dll")
	wnetAddConnection2W = mprDLL.NewProc("WNetAddConnection2W")
)

var connectionLocks = struct {
	sync.Mutex
	items map[string]*sync.Mutex
}{items: make(map[string]*sync.Mutex)}

var errConnectionBusy = errors.New("предыдущая попытка подключения сетевой папки ещё выполняется")

// Connect подключает только корень UNC-шары через Windows Network Provider.
// Пароль передаётся прямо WinAPI и не появляется в командной строке процесса.
func Connect(ctx context.Context, path, username, password string) error {
	root := uncRoot(path)
	if root == "" || strings.TrimSpace(username) == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	remoteName, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return fmt.Errorf("некорректный адрес сетевой папки: %w", err)
	}
	userName, err := windows.UTF16PtrFromString(strings.TrimSpace(username))
	if err != nil {
		return fmt.Errorf("некорректное имя пользователя сетевой папки: %w", err)
	}
	passwordValue, err := windows.UTF16PtrFromString(password)
	if err != nil {
		return fmt.Errorf("некорректный пароль сетевой папки: %w", err)
	}
	resource := netResource{Type: resourceTypeDisk, RemoteName: remoteName}
	connect := func() windows.Errno {
		result, _, _ := wnetAddConnection2W.Call(
			uintptr(unsafe.Pointer(&resource)), uintptr(unsafe.Pointer(passwordValue)),
			uintptr(unsafe.Pointer(userName)), 0,
		)
		return windows.Errno(result)
	}
	connectionLocks.Lock()
	lock := connectionLocks.items[strings.ToLower(root)]
	if lock == nil {
		lock = &sync.Mutex{}
		connectionLocks.items[strings.ToLower(root)] = lock
	}
	connectionLocks.Unlock()
	if !lock.TryLock() {
		return errConnectionBusy
	}
	completed := make(chan windows.Errno, 1)
	go func() {
		defer lock.Unlock()
		completed <- connect()
	}()
	var result windows.Errno
	select {
	case result = <-completed:
	case <-ctx.Done():
		return ctx.Err()
	}
	if result == errorSessionCredentialConflict {
		return fmt.Errorf("к %s уже подключена другая учётная запись Windows; отключите её вручную после завершения операций других программ: %w", root, result)
	}
	if result != windows.ERROR_SUCCESS {
		return fmt.Errorf("не удалось подключить сетевую папку %s: %w", root, result)
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
