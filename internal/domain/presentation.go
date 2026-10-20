package domain

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	presentationTimeLine = regexp.MustCompile(`^(\d{1,2})[.:](\d{2})\s*$`)
	presentationCodeTail = regexp.MustCompile(`\s*\((\d{1,10})\)\s*$`)
	presentationCodeAny  = regexp.MustCompile(`\((\d{1,10})\)`)
	presentationLeadDash = regexp.MustCompile(`^[-–—]\s*`)
)

// PresentedCorrespondent содержит ровно те сведения, которые показываются
// перед текстом перехвата. Number — номер карточки позывного в EAVC, а не
// искусственная нумерация по порядку появления в сообщении.
type PresentedCorrespondent struct {
	Number      int
	Code        string
	Label       string
	Description string
}

// PresentedLine — строка готового перехвата. Для строки времени Time=true.
type PresentedLine struct {
	Text string
	Time bool
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
	usedNumbers := make(map[int]bool)
	for _, code := range codes {
		if number := int(byCode[NormalizeCorrespondentCode(code)].Number); number > 0 {
			usedNumbers[number] = true
		}
	}
	nextNumber := 1
	numbers := make(map[string]int, len(codes))
	correspondents := make([]PresentedCorrespondent, 0, len(codes))
	for _, rawCode := range codes {
		code := NormalizeCorrespondentCode(rawCode)
		callsign := byCode[code]
		number := int(callsign.Number)
		if number <= 0 {
			for usedNumbers[nextNumber] {
				nextNumber++
			}
			number = nextNumber
			usedNumbers[number] = true
			nextNumber++
		}
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
	sort.SliceStable(correspondents, func(left, right int) bool {
		return correspondents[left].Number < correspondents[right].Number
	})

	lines := make([]PresentedLine, 0)
	for _, raw := range strings.Split(normalizeBlockText(content), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if match := presentationTimeLine.FindStringSubmatch(line); match != nil {
			hour, _ := strconv.Atoi(match[1])
			lines = append(lines, PresentedLine{Text: fmt.Sprintf("%02d:%s", hour, match[2]), Time: true})
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
	if candidate.Number > 0 && current.Number <= 0 {
		return true
	}
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
