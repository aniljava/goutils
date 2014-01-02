package generalutils

import (
	"fmt"
)

func ArrayIndex(array []string, key string) int {
	for i, v := range array {
		if v == key {
			return i
		}
	}
	return -1
}

func GetId(str string) string {
	//a-z|A-Z|0-9|-

	str = CompactTrim(str)
	str = strings.ToLower(str)
	result := ""

	arr := []byte(str)
	for _, b := range arr {
		if (b >= 48 && b <= 57) || (b >= 65 && b <= 90) || (b >= 97 && b <= 124) {
			result += string(b)
		} else {
			result += "-"
		}
	}
	return result
}

func ToString(iface interface{}) string {
	return fmt.Sprint(iface)
}

func Concat(ifaces ...interface{}) string {
	result := ""
	for _, iface := range ifaces {
		result += fmt.Sprint(iface)
	}
}
