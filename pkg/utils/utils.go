package utils

import (
	"strings"
)

func IsValidString(input *string) bool {
	return input != nil && strings.TrimSpace(*input) != ""
}
