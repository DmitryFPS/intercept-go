package domain

import (
	"reflect"
	"testing"
)

func TestPresentInterceptUsesLocalNumbersAndEAVCDescriptions(t *testing.T) {
	t.Parallel()
	content := "17.30\n-Монако Торбе на связь. (52325)\n-На связи. (52345)\n-До Олимпа дотянулися. (52325)"
	callsigns := []Callsign{
		{Number: 1, Code: "52345", Label: "Монако", Description: "ОД на КНП Гольназ, мужской голос, украинская речь"},
		{Number: 6, Code: "52325", Label: "Торба", Description: "Старший группы Гольназ, мужской голос, украинская речь"},
	}
	correspondents, lines := PresentIntercept(content, callsigns)
	wantCorrespondents := []PresentedCorrespondent{
		{Number: 1, Code: "52325", Label: "Торба", Description: "Старший группы Гольназ, мужской голос, украинская речь"},
		{Number: 2, Code: "52345", Label: "Монако", Description: "ОД на КНП Гольназ, мужской голос, украинская речь"},
	}
	if !reflect.DeepEqual(correspondents, wantCorrespondents) {
		t.Fatalf("корреспонденты отличаются:\nполучено: %#v\nожидалось: %#v", correspondents, wantCorrespondents)
	}
	wantLines := []PresentedLine{
		{Text: "17:30", Time: true},
		{Text: "/1/ Монако Торбе на связь."},
		{Text: "/2/ На связи."},
		{Text: "/1/ До Олимпа дотянулися."},
	}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Fatalf("реплики отличаются:\nполучено: %#v\nожидалось: %#v", lines, wantLines)
	}
}

func TestPresentInterceptNumbersFiveCorrespondentsOnceAcrossSeveralTimes(t *testing.T) {
	t.Parallel()
	content := "09.38\n-Первый доклад. (50002)\n-Ответ. (50001)\n10:14\n-Третий на связи. (50003)\n-Четвёртый принял. (50004)\n-Пятый подтверждает. (50005)\n-Повтор первого доклада. (50002)"
	callsigns := []Callsign{
		{Number: 41, Code: "50001", Label: "Первый"},
		{Number: 42, Code: "50002", Label: "Второй"},
		{Number: 81, Code: "50003", Label: "Третий"},
		{Number: 105, Code: "50004", Label: "Четвёртый"},
		{Number: 999, Code: "50005", Label: "Пятый"},
	}

	correspondents, lines := PresentIntercept(content, callsigns)
	if len(correspondents) != 5 {
		t.Fatalf("корреспондентов %d, ожидалось 5", len(correspondents))
	}
	for index, correspondent := range correspondents {
		if correspondent.Number != index+1 {
			t.Fatalf("номер корреспондента %q: %d, ожидалось %d", correspondent.Label, correspondent.Number, index+1)
		}
	}
	wantTail := []PresentedLine{
		{Text: "/3/ Третий на связи."},
		{Text: "/4/ Четвёртый принял."},
		{Text: "/5/ Пятый подтверждает."},
		{Text: "/1/ Повтор первого доклада."},
	}
	if !reflect.DeepEqual(lines[len(lines)-len(wantTail):], wantTail) {
		t.Fatalf("реплики второго времени отличаются:\nполучено: %#v\nожидалось: %#v", lines, wantTail)
	}
}

func TestPresentInterceptDoesNotPreferCardNumberOverDescription(t *testing.T) {
	t.Parallel()
	correspondents, _ := PresentIntercept("12.00\n-Доклад. (7)", []Callsign{
		{Code: "7", Label: "Марс", Description: "оператор"},
		{Number: 125, Code: "7"},
	})
	if len(correspondents) != 1 || correspondents[0].Number != 1 ||
		correspondents[0].Label != "Марс" || correspondents[0].Description != "оператор" {
		t.Fatalf("внутренний номер карточки повлиял на представление: %#v", correspondents)
	}
}
