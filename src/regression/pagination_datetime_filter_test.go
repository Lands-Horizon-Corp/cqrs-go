package regression

import (
	"context"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// This file verifies that a DataTypeDate/DataTypeTime filter's Value gets
// coerced from whatever string format the caller sent into a real
// time.Time before it's bound into the query — JSON has no native date
// type, so a client-supplied date/time filter always arrives as a string,
// and different clients reasonably send different formats (ISO 8601, plain
// "YYYY-MM-DD", US-style "MM/DD/YYYY", RFC1123, etc). widget.ExpiresAt
// (*time.Time) is the fixture column exercised throughout.

func TestPagination_HappyPath_DateFilterAcceptsManyDifferentStringFormats(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	cutoff := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	seedWidgets(t, read,
		widget{ID: "before", Name: "Before", ExpiresAt: new(cutoff.AddDate(0, 0, -5))},
		widget{ID: "after", Name: "After", ExpiresAt: new(cutoff.AddDate(0, 0, 5))},
	)

	formats := []string{
		"2024-06-15T00:00:00Z",          // RFC3339
		"2024-06-15",                    // bare ISO date
		"06/15/2024",                    // US-style MM/DD/YYYY
		"2024/06/15",                    // slash ISO date
		"Sat, 15 Jun 2024 00:00:00 UTC", // RFC1123
	}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			result, err := c.Pagination(ctx, domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{
					{Field: "expires_at", Mode: domains.ModeAfter, DataType: domains.DataTypeDate, Value: format},
				}},
			})
			if err != nil {
				t.Fatalf("Pagination returned error for format %q: %v", format, err)
			}
			if len(result.Data) != 1 || result.Data[0].ID != "after" {
				t.Fatalf("format %q: expected only [after], got %+v", format, result.Data)
			}
		})
	}
}

func TestPagination_HappyPath_DateRangeFilterAcceptsStringBoundsFromJSONShapedMap(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "One", ExpiresAt: new(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))},
		widget{ID: "w2", Name: "Two", ExpiresAt: new(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))},
		widget{ID: "w3", Name: "Three", ExpiresAt: new(time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC))},
	)

	// A range filter decoded from JSON arrives as map[string]any, and its
	// "from"/"to" values are themselves plain strings, not domains.RangeDate.
	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{
				Field: "expires_at", Mode: domains.ModeRange, DataType: domains.DataTypeDate,
				Value: map[string]any{"from": "2024-03-01", "to": "2024-09-01"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "w2" {
		t.Fatalf("expected only [w2] within the range, got %+v", result.Data)
	}
}

func TestPagination_HappyPath_DateInsideFilterAcceptsListOfStringValues(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	ctx := context.Background()
	seedWidgets(t, read,
		widget{ID: "w1", Name: "One", ExpiresAt: new(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))},
		widget{ID: "w2", Name: "Two", ExpiresAt: new(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))},
		widget{ID: "w3", Name: "Three", ExpiresAt: new(time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC))},
	)

	result, err := c.Pagination(ctx, domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{
				Field: "expires_at", Mode: domains.ModeInside, DataType: domains.DataTypeDate,
				Value: []any{"2024-01-01", "12/01/2024"}, // ISO + MM/DD/YYYY mixed in one list
			},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("expected 2 rows ([w1, w3]), got %+v", result.Data)
	}
}

func TestPagination_HappyPath_TimeOfDayFilterAcceptsSeveralFormats(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One"})

	// time.Kitchen-style ("3:04PM") and 24-hour "15:04" style inputs.
	formats := []string{"3:00 AM", "03:00"}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			if _, err := c.Pagination(context.Background(), domains.Pagination{
				Filter: domains.StructuredFilter{Filters: []domains.Filter{
					{Field: "name", Mode: domains.ModeEqual, DataType: domains.DataTypeTime, Value: format},
				}},
			}); err != nil {
				t.Fatalf("Pagination returned error for time-of-day format %q: %v", format, err)
			}
		})
	}
}

// TestPagination_HappyPath_DateFilterAcceptsAlreadyTypedTimeValue covers the
// case of a StructuredFilter built directly in Go (not decoded from JSON),
// where Value can already be a real time.Time rather than a string —
// coerceDateTimeFilterValue must pass it through unchanged instead of
// rejecting it for not being a string.
func TestPagination_HappyPath_DateFilterAcceptsAlreadyTypedTimeValue(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	cutoff := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	seedWidgets(t, read,
		widget{ID: "before", Name: "Before", ExpiresAt: new(cutoff.AddDate(0, 0, -5))},
		widget{ID: "after", Name: "After", ExpiresAt: new(cutoff.AddDate(0, 0, 5))},
	)

	result, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "expires_at", Mode: domains.ModeAfter, DataType: domains.DataTypeDate, Value: cutoff},
		}},
	})
	if err != nil {
		t.Fatalf("Pagination returned error for an already-typed time.Time value: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ID != "after" {
		t.Fatalf("expected only [after], got %+v", result.Data)
	}
}

func TestPagination_SadPath_DateRangeFilterRejectsUnrecognizedFromBound(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One", ExpiresAt: new(time.Now())})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{
				Field: "expires_at", Mode: domains.ModeRange, DataType: domains.DataTypeDate,
				Value: map[string]any{"from": "not-a-real-date", "to": "2024-09-01"},
			},
		}},
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized range \"from\" bound, got nil")
	}
}

func TestPagination_SadPath_DateRangeFilterRejectsUnrecognizedToBound(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One", ExpiresAt: new(time.Now())})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{
				Field: "expires_at", Mode: domains.ModeRange, DataType: domains.DataTypeDate,
				Value: map[string]any{"from": "2024-03-01", "to": "not-a-real-date"},
			},
		}},
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized range \"to\" bound, got nil")
	}
}

func TestPagination_SadPath_DateInsideFilterRejectsOneUnrecognizedElementInList(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One", ExpiresAt: new(time.Now())})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{
				Field: "expires_at", Mode: domains.ModeInside, DataType: domains.DataTypeDate,
				Value: []any{"2024-01-01", "not-a-real-date"},
			},
		}},
	})
	if err == nil {
		t.Fatal("expected an error for one unrecognized element in an Inside filter's list, got nil")
	}
}

func TestPagination_SadPath_UnrecognizedDateFilterValueReturnsClearError(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One", ExpiresAt: new(time.Now())})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "expires_at", Mode: domains.ModeAfter, DataType: domains.DataTypeDate, Value: "not-a-real-date"},
		}},
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized date string, got nil")
	}
}

func TestPagination_PoisonPill_DateFilterRejectsSQLInjectionAttemptAsUnrecognizedFormat(t *testing.T) {
	t.Parallel()
	c, read := newPaginationQueryTestCQRS(t)
	seedWidgets(t, read, widget{ID: "w1", Name: "One", ExpiresAt: new(time.Now())})

	_, err := c.Pagination(context.Background(), domains.Pagination{
		Filter: domains.StructuredFilter{Filters: []domains.Filter{
			{Field: "expires_at", Mode: domains.ModeAfter, DataType: domains.DataTypeDate, Value: "'; DROP TABLE widgets; --"},
		}},
	})
	if err == nil {
		t.Fatal("expected an error rejecting the unparseable injection payload as a date, got nil")
	}
	// The table must still exist and be queryable afterward.
	if _, err := c.Pagination(context.Background(), domains.Pagination{}); err != nil {
		t.Fatalf("table appears damaged after the rejected filter value: %v", err)
	}
}
