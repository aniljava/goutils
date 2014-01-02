package generalutils

func ArrayIndex(array []string, key string) {
	for i, v := range array {
		if v == key {
			return i
		}
	}
	return -1
}
