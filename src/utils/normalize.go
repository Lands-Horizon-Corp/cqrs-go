package utils

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	columnNameInvalidChars = regexp.MustCompile(`[^a-z_]+`)
	columnNameRepeatedSep  = regexp.MustCompile(`_+`)
)

// NormalizeColumnName turns arbitrary client-supplied column-name input
// (a filter or sort field name, typically) into the one canonical snake_case
// form a bun `column,...` tag would actually use, so lookups against it
// (BunColumnFieldIndex) aren't defeated by incidental formatting
// differences the caller didn't intend as meaningful:
//   - camelCase/PascalCase is converted to snake_case ("UserID" -> "user_id")
//   - the result is lowercased and trimmed
//   - anything that isn't a lowercase letter or underscore (spaces, digits,
//     punctuation, ...) becomes an underscore
//   - runs of underscores collapse to one, and leading/trailing underscores
//     are stripped
func NormalizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = camelToSnake(name)
	name = strings.ToLower(name)
	name = columnNameInvalidChars.ReplaceAllString(name, "_")
	name = columnNameRepeatedSep.ReplaceAllString(name, "_")
	return strings.Trim(name, "_")
}

// camelToSnake inserts an underscore at each camelCase/PascalCase word
// boundary and lowercases every letter it touches. A boundary is either a
// lowercase-or-digit run ending ("userName" -> the N before "ame") or the
// last letter of an acronym run just before it drops back to lowercase
// ("HTTPServer" -> the S before "erver", not every letter of "HTTP").
// Anything that isn't a letter (spaces, digits, punctuation) passes through
// unchanged here; NormalizeColumnName's later passes handle those.
func camelToSnake(s string) string {
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(runes) + len(runes)/3)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextIsLower) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
