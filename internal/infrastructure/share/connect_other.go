//go:build !windows

package share

import "context"

// Connect ничего не подключает вне Windows; локальные пути уже доступны системе.
func Connect(_ context.Context, _, _, _ string) error { return nil }
