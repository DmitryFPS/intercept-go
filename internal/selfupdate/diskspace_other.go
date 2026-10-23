//go:build !windows

package selfupdate

import "errors"

// На поддерживаемой промышленной платформе Windows выполняется точная
// проверка. Эта реализация сохраняет возможность сборки вспомогательных тестов
// на других ОС.
func ensureFreeSpace(_ string, required int64) error {
	if required < 0 {
		return errors.New("недопустимый требуемый объём свободного места")
	}
	return nil
}
