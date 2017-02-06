package gorpx

import (
	"strings"
)

func Concat(dbType string, args ...string) string {
	if dbType == "sqlite3" {
		return strings.Join(args, " || ")
	}
	return " CONCAT(" + strings.Join(args, ", ") + ")"
}
