//go:build !windows

package autostart

func Ensure(_, _ string) error { return nil }
