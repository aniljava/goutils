package sysutils

import (
	"os/exec"
)

func Exec(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	data, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(data), nil

}
