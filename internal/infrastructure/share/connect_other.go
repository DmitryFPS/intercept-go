//go:build !windows

package share

import "context"

func Connect(_ context.Context, _, _, _ string) error { return nil }
