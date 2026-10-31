package sqlite

import "time"

// SQLite julianday() округляет дробную часть до миллисекунд. Индексный фильтр
// по нему дополняется точным сравнением UTC RFC3339Nano с девятью цифрами.
// column всегда задан в коде, а не берётся из входящих данных.
func sqliteNanoTimeExpr(column string) string {
	return "substr(" + column + ",1,19)||'.'||substr(rtrim(substr(" + column + ",21),'Z')||'000000000',1,9)||'Z'"
}

func sqliteNanoTimeKey(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
