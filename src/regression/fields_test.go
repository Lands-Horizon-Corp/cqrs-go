package regression

import (
	"testing"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
)

type fieldsTestEntity struct {
	ID        string `bun:"id,pk"`
	Name      string `bun:"name"`
	Age       int    `bun:"age"`
	Untagged  string
	Skipped   string `bun:"-"`
	secret    string `bun:"secret"` //nolint:unused // exercised via reflection in poison pill test
	SecretPtr *string
}

func TestBunColumnFieldIndex_HappyPath_ResolvesMatchingColumn(t *testing.T) {
	idx := utils.BunColumnFieldIndex[fieldsTestEntity]("id")
	if idx != 0 {
		t.Fatalf("expected index 0 for column 'id', got %d", idx)
	}

	idx = utils.BunColumnFieldIndex[fieldsTestEntity]("age")
	if idx != 2 {
		t.Fatalf("expected index 2 for column 'age', got %d", idx)
	}
}

func TestFieldValueAt_HappyPath_ReadsStringAndNonString(t *testing.T) {
	entity := fieldsTestEntity{ID: "abc-123", Name: "widget", Age: 7}

	idIdx := utils.BunColumnFieldIndex[fieldsTestEntity]("id")
	if got := utils.FieldValueAt(&entity, idIdx); got != "abc-123" {
		t.Errorf("expected 'abc-123', got %q", got)
	}

	ageIdx := utils.BunColumnFieldIndex[fieldsTestEntity]("age")
	if got := utils.FieldValueAt(&entity, ageIdx); got != "7" {
		t.Errorf("expected '7', got %q", got)
	}
}

func TestBunColumnFieldIndex_SadPath_NoMatchOrNotStruct(t *testing.T) {
	t.Run("Unknown Column", func(t *testing.T) {
		idx := utils.BunColumnFieldIndex[fieldsTestEntity]("does_not_exist")
		if idx != -1 {
			t.Errorf("expected -1 for unknown column, got %d", idx)
		}
	})

	t.Run("Untagged Field Never Matches", func(t *testing.T) {
		idx := utils.BunColumnFieldIndex[fieldsTestEntity]("")
		if idx != -1 {
			t.Errorf("expected -1 for empty column against untagged/dash fields, got %d", idx)
		}
	})

	t.Run("Explicitly Dashed Tag Skipped", func(t *testing.T) {
		idx := utils.BunColumnFieldIndex[fieldsTestEntity]("-")
		if idx != -1 {
			t.Errorf("expected -1, '-' tags must be treated as untagged, got %d", idx)
		}
	})

	t.Run("Non-Struct Type Parameter", func(t *testing.T) {
		idx := utils.BunColumnFieldIndex[string]("id")
		if idx != -1 {
			t.Errorf("expected -1 for non-struct type parameter, got %d", idx)
		}
	})
}

func TestFieldValueAt_SadPath_NilAndOutOfRange(t *testing.T) {
	t.Run("Nil Data", func(t *testing.T) {
		if got := utils.FieldValueAt[fieldsTestEntity](nil, 0); got != "" {
			t.Errorf("expected empty string for nil data, got %q", got)
		}
	})

	t.Run("Negative Index", func(t *testing.T) {
		entity := fieldsTestEntity{ID: "x"}
		if got := utils.FieldValueAt(&entity, -1); got != "" {
			t.Errorf("expected empty string for negative index, got %q", got)
		}
	})

	t.Run("Out Of Range Index Does Not Panic", func(t *testing.T) {
		entity := fieldsTestEntity{ID: "x"}
		if got := utils.FieldValueAt(&entity, 999); got != "" {
			t.Errorf("expected empty string for out-of-range index, got %q", got)
		}
	})
}

// TestFieldValueAt_PoisonPill_UnexportedFieldNeverSelectedOrPanics guards
// against two ways a hostile/mistagged struct could crash this reflection
// path: BunColumnFieldIndex must never return the index of an unexported
// field (even if it carries a matching `bun` tag), and even if some other
// caller hands FieldValueAt an unexported field's index directly, it must
// return "" rather than panic via reflect.Value.Interface on a value that
// CanInterface() reports false for.
func TestFieldValueAt_PoisonPill_UnexportedFieldNeverSelectedOrPanics(t *testing.T) {
	idx := utils.BunColumnFieldIndex[fieldsTestEntity]("secret")
	if idx != -1 {
		t.Fatalf("expected unexported field with matching tag to be skipped, got index %d", idx)
	}

	// Field 5 is the unexported `secret` field regardless of resolution;
	// calling FieldValueAt directly on it must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("FieldValueAt panicked on unexported field: %v", r)
		}
	}()
	entity := fieldsTestEntity{secret: "MALICIOUS_POISON_PAYLOAD"}
	if got := utils.FieldValueAt(&entity, 5); got != "" {
		t.Errorf("expected empty string for unexported field, got %q (poison leaked)", got)
	}
}

// TestFieldValueAt_PoisonPill_NilPointerFieldIsAbsentNotTheStringNil is the
// utils-level companion to the nilpointer_test.go entity-ID case: any
// nil pointer field (not just an ID column) must resolve to "", never the
// literal text "<nil>".
func TestFieldValueAt_PoisonPill_NilPointerFieldIsAbsentNotTheStringNil(t *testing.T) {
	entity := fieldsTestEntity{ID: "x", SecretPtr: nil}
	idx := 6 // SecretPtr's field index
	if got := utils.FieldValueAt(&entity, idx); got != "" {
		t.Errorf(`expected "" for a nil pointer field, got %q`, got)
	}

	val := "not nil"
	entity.SecretPtr = &val
	if got := utils.FieldValueAt(&entity, idx); got != "not nil" {
		t.Errorf("expected dereferenced value %q, got %q", "not nil", got)
	}
}
