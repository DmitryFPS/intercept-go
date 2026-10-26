package application

import "errors"

// PermanentDeliveryError означает, что повтор неизменённого задания уже не
// может привести к успеху. Такое задание нужно завершить с видимой причиной,
// чтобы оно не удерживало остальные сообщения своей очереди.
type PermanentDeliveryError struct{ Err error }

func (err *PermanentDeliveryError) Error() string { return err.Err.Error() }
func (err *PermanentDeliveryError) Unwrap() error { return err.Err }

// NewPermanentDeliveryError помечает ошибку внешнего получателя как
// окончательную. nil остаётся nil, чтобы адаптеры могли безопасно оборачивать
// результат условной операции.
func NewPermanentDeliveryError(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentDeliveryError{Err: err}
}

// IsPermanentDeliveryError отличает неисправимое задание от временного сбоя
// сети, ограничения частоты или недоступности внешнего сервиса.
func IsPermanentDeliveryError(err error) bool {
	var permanent *PermanentDeliveryError
	return errors.As(err, &permanent)
}
