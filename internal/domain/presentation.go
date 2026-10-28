package domain

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	presentationCodeTail = regexp.MustCompile(`\s*\((\d{1,10})\)\s*$`)
	presentationCodeAny  = regexp.MustCompile(`\((\d{1,10})\)`)
	presentationLeadDash = regexp.MustCompile(`^[-–—]\s*`)
)

// PresentedCorrespondent содержит ровно те сведения, которые показываются
// перед текстом перехвата. Number — локальный порядковый номер корреспондента
// в текущем перехвате. Внутренний номер карточки EAVC наружу не выводится.
type PresentedCorrespondent struct {
	Number      int
	Code        string
	Label       string
	Description string
}

// PresentedLine — строка готового перехвата. Для строки времени Time=true.
// InternalTime отличает помеченное оператором время с #, которое остаётся
// внутри текущего временного блока и только оформляется по-разному в каналах.
type PresentedLine struct {
	Text         string
	Time         bool
	InternalTime bool
}

// PresentIntercept преобразует бланк EAVC в общий для Telegram и Word вид.
// Это не даёт двум каналам доставки по-разному нумеровать корреспондентов или
// оставлять в репликах внутренние числовые коды.
func PresentIntercept(content string, callsigns []Callsign) ([]PresentedCorrespondent, []PresentedLine) {
	byCode := make(map[string]Callsign)
	for _, callsign := range callsigns {
		code := NormalizeCorrespondentCode(callsign.Code)
		if code == "" {
			continue
		}
		current, exists := byCode[code]
		if !exists || richerCallsign(callsign, current) {
			byCode[code] = callsign
		}
	}

	codes := ExtractCorrespondentCodes(content)
	numbers := make(map[string]int, len(codes))
	correspondents := make([]PresentedCorrespondent, 0, len(codes))
	for index, rawCode := range codes {
		code := NormalizeCorrespondentCode(rawCode)
		callsign := byCode[code]
		number := index + 1
		numbers[code] = number
		label := strings.TrimSpace(callsign.Label)
		if label == "" {
			label = "н/у"
		}
		description := strings.TrimSpace(callsign.Description)
		if description == "" {
			description = strings.TrimSpace(callsign.Tag)
		}
		correspondents = append(correspondents, PresentedCorrespondent{
			Number: number, Code: rawCode, Label: label, Description: description,
		})
	}

	lines := make([]PresentedLine, 0)
	for _, raw := range strings.Split(normalizeBlockText(content), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if normalized, _, ok := CanonicalTimeHeader(line); ok {
			lines = append(lines, PresentedLine{Text: normalized, Time: true})
			continue
		}
		body := presentationLeadDash.ReplaceAllString(line, "")
		code := ""
		if match := presentationCodeTail.FindStringSubmatchIndex(body); match != nil {
			code = body[match[2]:match[3]]
			body = strings.TrimSpace(body[:match[0]])
		} else if match := presentationCodeAny.FindStringSubmatchIndex(body); match != nil {
			code = body[match[2]:match[3]]
			body = strings.TrimSpace(body[:match[0]] + body[match[1]:])
		}
		if number := numbers[NormalizeCorrespondentCode(code)]; number > 0 {
			body = "/" + strconv.Itoa(number) + "/ " + body
		}
		lines = append(lines, PresentedLine{Text: strings.TrimSpace(body)})
	}
	return correspondents, lines
}

func richerCallsign(candidate, current Callsign) bool {
	if strings.TrimSpace(candidate.Description) != "" && strings.TrimSpace(current.Description) == "" {
		return true
	}
	return strings.TrimSpace(candidate.Label) != "" && strings.TrimSpace(current.Label) == ""
}

// NormalizeCorrespondentCode приводит цифровой ID к виду, используемому EAVC.
func NormalizeCorrespondentCode(value string) string {
	raw := strings.TrimSpace(value)
	value = strings.TrimLeft(raw, "0")
	if value == "" && raw != "" {
		return "0"
	}
	return value
}
