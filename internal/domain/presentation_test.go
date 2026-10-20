package domain

import (
	"reflect"
	"testing"
)

func TestPresentInterceptUsesEAVCCardNumbersAndDescriptions(t *testing.T) {
	t.Parallel()
	content := "17.30\n-Монако Торбе на связь. (52325)\n-На связи. (52345)\n-До Олимпа дотянулися. (52325)"
	callsigns := []Callsign{
		{Number: 1, Code: "52345", Label: "Монако", Description: "ОД на КНП Гольназ, мужской голос, украинская речь"},
		{Number: 6, Code: "52325", Label: "Торба", Description: "Старший группы Гольназ, мужской голос, украинская речь"},
	}
	correspondents, lines := PresentIntercept(content, callsigns)
	wantCorrespondents := []PresentedCorrespondent{
		{Number: 1, Code: "52345", Label: "Монако", Description: "ОД на КНП Гольназ, мужской голос, украинская речь"},
		{Number: 6, Code: "52325", Label: "Торба", Description: "Старший группы Гольназ, мужской голос, украинская речь"},
	}
	if !reflect.DeepEqual(correspondents, wantCorrespondents) {
		t.Fatalf("корреспонденты отличаются:\nполучено: %#v\nожидалось: %#v", correspondents, wantCorrespondents)
	}
	wantLines := []PresentedLine{
		{Text: "17:30", Time: true},
		{Text: "/6/ Монако Торбе на связь."},
		{Text: "/1/ На связи."},
		{Text: "/6/ До Олимпа дотянулися."},
	}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Fatalf("реплики отличаются:\nполучено: %#v\nожидалось: %#v", lines, wantLines)
	}
}
