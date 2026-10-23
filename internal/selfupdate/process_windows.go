//go:build windows

package selfupdate

import (
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

func setHiddenProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func waitForProcessExit(pid int, timeout time.Duration) error {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return nil
	}
	defer windows.CloseHandle(handle)
	status, err := windows.WaitForSingleObject(handle, uint32(timeout/time.Millisecond))
	if err != nil {
		return err
	}
	if status == uint32(windows.WAIT_TIMEOUT) {
		return syscall.ETIMEDOUT
	}
	return nil
}
