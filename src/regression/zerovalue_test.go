package regression

import (
	"testing"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// TestZeroValue_BoolCannotDistinguishAbsentFromFalse is the empirical
// version of "why can't I update a plain bool to false": at the JSON layer
// (what Debezium payloads and any JSON request body go through), a
// non-pointer bool field decodes to the exact same Go value (false)
// whether the JSON omitted the key entirely or explicitly sent false.
func TestZeroValue_BoolCannotDistinguishAbsentFromFalse(t *testing.T) {
	t.Parallel()
	var absent widget
	if err := sonic.Unmarshal([]byte(`{"id":"w1","name":"n"}`), &absent); err != nil {
		t.Fatalf("unmarshal (absent): %v", err)
	}

	var explicitFalse widget
	if err := sonic.Unmarshal([]byte(`{"id":"w1","name":"n","active":false}`), &explicitFalse); err != nil {
		t.Fatalf("unmarshal (explicit false): %v", err)
	}

	if absent.Active != false || explicitFalse.Active != false {
		t.Fatalf("expected both to decode Active=false, got absent=%v explicit=%v", absent.Active, explicitFalse.Active)
	}
	if absent.Active != explicitFalse.Active {
		t.Fatal("expected absent and explicit-false to be indistinguishable for a plain bool field")
	}
}

// TestZeroValue_PointerFieldsDistinguishAbsentFromExplicitZero shows the
// fix: a pointer field decodes to nil when the JSON key is absent OR
// explicitly null, but to a non-nil pointer to the zero value when the
// JSON explicitly sends that zero value. This holds across every pointer
// type on widget, not just *bool.
func TestZeroValue_PointerFieldsDistinguishAbsentFromExplicitZero(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		json  string
		check func(t *testing.T, w widget)
	}{
		{
			name: "Bool: Key Absent -> nil",
			json: `{"id":"w1","name":"n"}`,
			check: func(t *testing.T, w widget) {
				if w.Featured != nil {
					t.Fatalf("expected nil, got %v", *w.Featured)
				}
			},
		},
		{
			name: "Bool: Explicit null -> nil",
			json: `{"id":"w1","name":"n","featured":null}`,
			check: func(t *testing.T, w widget) {
				if w.Featured != nil {
					t.Fatalf("expected nil, got %v", *w.Featured)
				}
			},
		},
		{
			name: "Bool: Explicit false -> &false",
			json: `{"id":"w1","name":"n","featured":false}`,
			check: func(t *testing.T, w widget) {
				if w.Featured == nil || *w.Featured {
					t.Fatalf("expected &false, got %v", w.Featured)
				}
			},
		},
		{
			name: "String: Key Absent -> nil",
			json: `{"id":"w1","name":"n"}`,
			check: func(t *testing.T, w widget) {
				if w.Notes != nil {
					t.Fatalf("expected nil, got %v", *w.Notes)
				}
			},
		},
		{
			name: "String: Explicit empty string -> &\"\"",
			json: `{"id":"w1","name":"n","notes":""}`,
			check: func(t *testing.T, w widget) {
				if w.Notes == nil || *w.Notes != "" {
					t.Fatalf("expected &\"\", got %v", w.Notes)
				}
			},
		},
		{
			name: "Int: Key Absent -> nil",
			json: `{"id":"w1","name":"n"}`,
			check: func(t *testing.T, w widget) {
				if w.Priority != nil {
					t.Fatalf("expected nil, got %v", *w.Priority)
				}
			},
		},
		{
			name: "Int: Explicit zero -> &0",
			json: `{"id":"w1","name":"n","priority":0}`,
			check: func(t *testing.T, w widget) {
				if w.Priority == nil || *w.Priority != 0 {
					t.Fatalf("expected &0, got %v", w.Priority)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got widget
			if err := sonic.Unmarshal([]byte(tc.json), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			tc.check(t, got)
		})
	}
}

// TestZeroValue_PoisonPill_CDCEnvelopeWithMissingBoolField mirrors an
// actual Debezium "after" payload for an update where the source row's
// active column happens to be false: this is what Run() unmarshals every
// incoming Kafka message into. Once unmarshalled, there is no signal left
// distinguishing "active was really sent as false" from "active was
// missing from a malformed/partial message" — both look identical.
func TestZeroValue_PoisonPill_CDCEnvelopeWithMissingBoolField(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"event_id": "evt-1",
		"change_type": 1,
		"payload": {"id": "w1", "name": "n"}
	}`)

	var env domains.CQRSQueuePayload[widget]
	if err := sonic.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Payload.Active {
		t.Fatal("sanity check failed: expected Active=false for a payload that never mentioned it")
	}
}
