//go:build !windows

package selfupdate

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func atomicReplace(source, target string) error { return os.Rename(source, target) }

func setHiddenProcess(command *exec.Cmd) {}

func waitForProcessExit(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		process, err := os.FindProcess(pid)
		if err != nil || process.Signal(syscall.Signal(0)) != nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return syscall.ETIMEDOUT
}
