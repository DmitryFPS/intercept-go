package selfupdate

import (
	"strconv"
	"strings"
)

func compareVersions(left, right string) int {
	type version struct {
		numbers []int
		label   string
		labelNo int
	}
	parse := func(value string) version {
		value = strings.TrimPrefix(strings.TrimSpace(value), "v")
		pieces := strings.SplitN(value, "-", 2)
		parts := strings.Split(pieces[0], ".")
		result := version{numbers: make([]int, len(parts))}
		for index, part := range parts {
			result.numbers[index], _ = strconv.Atoi(part)
		}
		if len(pieces) == 2 {
			label := strings.ToLower(strings.TrimSpace(pieces[1]))
			for index, char := range label {
				if char >= '0' && char <= '9' {
					result.label = strings.TrimRight(label[:index], ".-_")
					result.labelNo, _ = strconv.Atoi(label[index:])
					return result
				}
			}
			result.label = label
		}
		return result
	}
	a, b := parse(left), parse(right)
	length := len(a.numbers)
	if len(b.numbers) > length {
		length = len(b.numbers)
	}
	for index := 0; index < length; index++ {
		var av, bv int
		if index < len(a.numbers) {
			av = a.numbers[index]
		}
		if index < len(b.numbers) {
			bv = b.numbers[index]
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	if a.label == "" && b.label != "" {
		return 1
	}
	if a.label != "" && b.label == "" {
		return -1
	}
	if a.label < b.label {
		return -1
	}
	if a.label > b.label {
		return 1
	}
	if a.labelNo < b.labelNo {
		return -1
	}
	if a.labelNo > b.labelNo {
		return 1
	}
	return 0
}
