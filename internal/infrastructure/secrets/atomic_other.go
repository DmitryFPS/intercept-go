//go:build !windows

package secrets

import "os"

func replaceAtomic(target, replacement string) error { return os.Rename(replacement, target) }
