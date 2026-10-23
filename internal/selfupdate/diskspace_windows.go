//go:build windows

package selfupdate

import (
	"fmt"
	"syscall"
	"unsafe"
)

var getDiskFreeSpaceEx = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

func ensureFreeSpace(path string, required int64) error {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	var available uint64
	result, _, callErr := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pointer)),
		uintptr(unsafe.Pointer(&available)),
		0,
		0,
	)
	if result == 0 {
		return fmt.Errorf("не удалось проверить свободное место: %w", callErr)
	}
	if required < 0 || uint64(required) > available {
		return fmt.Errorf("недостаточно свободного места для безопасного обновления: требуется ещё не менее %d МБ", (required+1024*1024-1)/(1024*1024))
	}
	return nil
}
