//go:build !windows

package singleinstance

import "sync"

var (
	mu     sync.Mutex
	active = make(map[string]bool)
)

func Acquire(name string) (func(), bool, error) {
	mu.Lock()
	defer mu.Unlock()
	if active[name] {
		return func() {}, true, nil
	}
	active[name] = true
	return func() { mu.Lock(); delete(active, name); mu.Unlock() }, false, nil
}
