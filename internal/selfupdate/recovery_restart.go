package selfupdate

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

// StartRecoveryRestart launches the same trusted executable. The child waits
// for the current process to release its global mutex, log, port and SQLite
// handles before starting one ordinary worker cycle.
func StartRecoveryRestart(executable, baseDir string, oldPID int) error {
	command := exec.Command(executable,
		"--no-browser",
		"--restart-after-pid="+strconv.Itoa(oldPID),
		"--storage-recovery-process",
	)
	command.Dir = baseDir
	setHiddenProcess(command)
	if err := command.Start(); err != nil {
		return fmt.Errorf("запустить восстановление процесса: %w", err)
	}
	return command.Process.Release()
}

// WaitForRecoveryParent keeps the replacement process away from files that
// are still owned by its parent.
func WaitForRecoveryParent(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := waitForProcessExit(pid, 2*time.Minute); err != nil {
		return fmt.Errorf("дождаться завершения предыдущего процесса: %w", err)
	}
	return nil
}
