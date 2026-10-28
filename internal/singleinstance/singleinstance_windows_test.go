//go:build windows

package singleinstance

import "testing"

func TestWindowsMutexUsesMachineWideNamespace(t *testing.T) {
	if got := globalMutexName("EAVC-Connect-Station"); got != `Global\EAVC-Connect-Station` {
		t.Fatalf("mutex is not machine-wide: %q", got)
	}
}

func TestAcquireRejectsSecondInstance(t *testing.T) {
	releaseFirst, alreadyRunning, err := Acquire("EAVC-Connect-Test-Second-Instance")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFirst()
	if alreadyRunning {
		t.Fatal("first instance was reported as already running")
	}

	releaseSecond, alreadyRunning, err := Acquire("EAVC-Connect-Test-Second-Instance")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseSecond()
	if !alreadyRunning {
		t.Fatal("second instance acquired the same machine-wide mutex")
	}
}
