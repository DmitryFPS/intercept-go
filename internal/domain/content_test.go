package domain

import (
	"strings"
	"testing"
)

// Несколько сообщений с одинаковым временем должны сохраниться отдельными блоками и в исходном порядке.
func TestSplitContentBlocksKeepsOrderAndDuplicateTimes(t *testing.T) {
	t.Parallel()
	blocks := SplitContentBlocks("заголовок\n12.00\n-Первое (123)\n12.00\n-Второе (456)")
	if len(blocks) != 3 {
		t.Fatalf("ожидалось три блока: %#v", blocks)
	}
	if blocks[0].Key != "начало" || blocks[1].Key != "12:00" || blocks[2].Key != "12:00#2" {
		t.Fatalf("неверные ключи: %#v", blocks)
	}
	if blocks[0].Sequence != 0 || blocks[2].Sequence != 2 {
		t.Fatalf("нарушен порядок: %#v", blocks)
	}
	codes := ExtractCorrespondentCodes(blocks[1].Text + " (123) (789)")
	if len(codes) != 2 || codes[0] != "123" || codes[1] != "789" {
		t.Fatalf("неверные коды: %#v", codes)
	}
}

func TestSplitContentBlocksMakesEveryMarkedTimeIndependent(t *testing.T) {
	t.Parallel()
	raw := "10.15\n-Начало\n10.18#\n-Уточнение\n10.28 # 1\n-Продолжение #дословно\n10.41\n-Новый блок"
	blocks := SplitContentBlocks(raw)
	if len(blocks) != 4 {
		t.Fatalf("каждая строка времени должна стать блоком: %#v", blocks)
	}
	if blocks[0].Key != "10:15" || blocks[1].Key != "10:18" ||
		blocks[2].Key != "10:28" || blocks[3].Key != "10:41" {
		t.Fatalf("неверные границы блоков: %#v", blocks)
	}
	if blocks[2].Text != "10:28\n-Продолжение #дословно" {
		t.Fatalf("решётка в реплике не сохранена дословно: %q", blocks[2].Text)
	}
	if blocks[0].Hash == "" || blocks[1].Hash == "" || blocks[0].Hash == blocks[1].Hash {
		t.Fatalf("отпечатки блоков не сохранены: %#v", blocks)
	}
}

func TestSplitContentBlocksAcceptsColonAndRejectsInvalidTimeBoundary(t *testing.T) {
	t.Parallel()
	blocks := SplitContentBlocks("10:15\n-Начало\n24.00\n-Не время\n10.60\n-Тоже не время\n10.41\n-Новый блок")
	if len(blocks) != 2 || blocks[0].Key != "10:15" || blocks[1].Key != "10:41" {
		t.Fatalf("границы времени определены неверно: %#v", blocks)
	}
	if !strings.Contains(blocks[0].Text, "24.00\n-Не время\n10.60\n-Тоже не время") {
		t.Fatalf("ошибочное время потерялось или стало границей: %q", blocks[0].Text)
	}
}

func TestCanonicalTimeHeaderRequiresWholeLine(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"10.10", "10:10", " 10.10 # ", "10.10#1", "10.10 # 12 "} {
		canonical, minutes, ok := CanonicalTimeHeader(value)
		if !ok || canonical != "10:10" || minutes != 610 {
			t.Fatalf("допустимый заголовок %q не распознан: %q %d %v", value, canonical, minutes, ok)
		}
	}
	for _, value := range []string{"10.10# текст", "встреча 10.10#", "#1", "24.00#", "10.60 #1"} {
		if _, _, ok := CanonicalTimeHeader(value); ok {
			t.Fatalf("обычный текст ошибочно признан временем: %q", value)
		}
	}
}
