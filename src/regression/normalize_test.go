package regression

import (
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

func TestNormalizeColumnName_HappyPath_CamelAndPascalCaseConvertToSnakeCase(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"UserID":     "user_id",
		"userName":   "user_name",
		"HTTPServer": "http_server",
		"PascalCase": "pascal_case",
		"ID":         "id",
		"A":          "a",
	}
	for in, want := range cases {
		if got := utils.NormalizeColumnName(in); got != want {
			t.Errorf("NormalizeColumnName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeColumnName_HappyPath_WhitespaceAndSpecialCharactersBecomeSingleUnderscore(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"  full name  ": "full_name",
		"full  name":    "full_name", // double space collapses to one underscore
		"full--name":    "full_name",
		"full___name":   "full_name", // already-repeated underscores also collapse
		"order-date":    "order_date",
		"_leading_":     "leading",
		"trailing_":     "trailing",
	}
	for in, want := range cases {
		if got := utils.NormalizeColumnName(in); got != want {
			t.Errorf("NormalizeColumnName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeColumnName_PoisonPill_DigitsAreNotAlphabetSoTheyCollapseAway(t *testing.T) {
	t.Parallel()
	// Only lowercase letters and underscore are treated as valid column
	// characters — digits are stripped (as a run, collapsing to one
	// underscore) same as any other disallowed character.
	cases := map[string]string{
		"field123":  "field",
		"field 1 2": "field",
		"2fast":     "fast",
	}
	for in, want := range cases {
		if got := utils.NormalizeColumnName(in); got != want {
			t.Errorf("NormalizeColumnName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeColumnName_ZeroValue_EmptyAndAlreadyNormalizedInputsAreStable(t *testing.T) {
	t.Parallel()
	if got := utils.NormalizeColumnName(""); got != "" {
		t.Errorf("NormalizeColumnName(\"\") = %q, want \"\"", got)
	}
	if got := utils.NormalizeColumnName("already_snake_case"); got != "already_snake_case" {
		t.Errorf("NormalizeColumnName(already-normalized) = %q, want unchanged", got)
	}
}
