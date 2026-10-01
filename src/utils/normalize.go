package utils

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	columnNameInvalidChars = regexp.MustCompile(`[^a-z_]+`)
	columnNameRepeatedSep  = regexp.MustCompile(`_+`)
	pascalCaseWordSep      = regexp.MustCompile(`[^A-Za-z0-9]+`)
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

// ToPascalCase turns arbitrary client-supplied relation-name input (a
// preload path segment, typically) into the PascalCase form a bun struct
// field name — and therefore a bun `rel:...` relation name — actually
// takes, the same way NormalizeColumnName does for snake_case column
// names:
//   - the string is split on any run of non-alphanumeric characters
//     (underscore, hyphen, space, ...), each resulting word's first letter
//     is uppercased, and the words are joined back with no separator:
//     "author_posts" / "author-posts" / "author posts" -> "AuthorPosts"
//   - a word that arrived with no separator at all is left otherwise
//     untouched beyond its first letter, so an already-PascalCase or
//     camelCase input's internal capitalization survives intact instead of
//     being flattened: "authorPosts" -> "AuthorPosts" (the inner "P" is
//     preserved, not lowercased first and re-capitalized), and
//     "AuthorPosts" is unchanged
//
// Unlike NormalizeColumnName, this never lowercases: bun relation names are
// case-sensitive Go exported field names, not case-insensitive SQL
// identifiers, so collapsing case here would make a correctly-cased
// relation unresolvable instead of fixing an incorrectly-cased one.
func ToPascalCase(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	words := pascalCaseWordSep.Split(s, -1)
	var b strings.Builder
	b.Grow(len(s))
	for _, w := range words {
		if w == "" {
			continue
		}
		r := []rune(w)
		b.WriteRune(unicode.ToUpper(r[0]))
		b.WriteString(string(r[1:]))
	}
	return b.String()
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
