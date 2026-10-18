package domain

import "testing"

// Несколько сообщений с одинаковым временем должны сохраниться отдельными блоками и в исходном порядке.
func TestSplitContentBlocksKeepsOrderAndDuplicateTimes(t *testing.T) {
	t.Parallel()
	blocks := SplitContentBlocks("заголовок\n12.00\n-Первое (123)\n12.00\n-Второе (456)")
	if len(blocks) != 3 {
		t.Fatalf("ожидалось три блока: %#v", blocks)
	}
	if blocks[0].Key != "начало" || blocks[1].Key != "12.00" || blocks[2].Key != "12.00#2" {
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
