package regression

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// transformDebezium and its supporting types are test-only glue, not
// shipped library code — this project ships domains.MessageBrokerService
// as an interface and deliberately does not bundle a Debezium adapter, so
// consumers bring their own. This version exists purely to prove, in this
// test suite, that Run can be fed from a real Debezium connector's actual
// output once *something* does this reshaping — see
// integration_debezium_test.go for the full round trip.
//
// The envelope shape here (before/after/source/op/ts_ms, with
// source.lsn/source.txId) was captured directly from a live Debezium
// 2.7.3.Final Postgres connector (see local/docker-compose) against real
// insert/update/delete statements — not assumed from generic docs. One
// real caveat, also confirmed against the live connector: with the
// default REPLICA IDENTITY (primary key only), a delete event's "before"
// only reliably contains the primary key column — every other field comes
// back zero-valued, not the row's actual prior values.
const (
	debeziumOpCreate   = "c"
	debeziumOpUpdate   = "u"
	debeziumOpDelete   = "d"
	debeziumOpSnapshot = "r"
)

type debeziumSource struct {
	Table string `json:"table"`
	LSN   int64  `json:"lsn"`
}

type debeziumEnvelope[T any] struct {
	Before *T             `json:"before"`
	After  *T             `json:"after"`
	Source debeziumSource `json:"source"`
	Op     string         `json:"op"`
}

var errDebeziumTombstone = errors.New("debezium: tombstone record, nothing to apply")

func transformDebezium[T any](raw []byte) (domains.CQRSQueuePayload[T], error) {
	var env debeziumEnvelope[T]
	if err := sonic.Unmarshal(raw, &env); err != nil {
		return domains.CQRSQueuePayload[T]{}, fmt.Errorf("unmarshalling debezium envelope: %w", err)
	}
	if env.Before == nil && env.After == nil {
		return domains.CQRSQueuePayload[T]{}, errDebeziumTombstone
	}

	var changeType domains.ChangeType
	var payload T
	switch env.Op {
	case debeziumOpCreate, debeziumOpSnapshot:
		changeType = domains.ChangeTypeCreated
		if env.After == nil {
			return domains.CQRSQueuePayload[T]{}, fmt.Errorf("debezium: op=%q with no 'after' payload", env.Op)
		}
		payload = *env.After
	case debeziumOpUpdate:
		changeType = domains.ChangeTypeUpdated
		if env.After == nil {
			return domains.CQRSQueuePayload[T]{}, fmt.Errorf("debezium: op=%q with no 'after' payload", env.Op)
		}
		payload = *env.After
	case debeziumOpDelete:
		changeType = domains.ChangeTypeDeleted
		if env.Before == nil {
			return domains.CQRSQueuePayload[T]{}, fmt.Errorf("debezium: op=%q with no 'before' payload", env.Op)
		}
		payload = *env.Before
	default:
		return domains.CQRSQueuePayload[T]{}, fmt.Errorf("debezium: unrecognized op %q", env.Op)
	}

	return domains.CQRSQueuePayload[T]{
		EventID:    fmt.Sprintf("%s-%d", env.Source.Table, env.Source.LSN),
		ChangeType: changeType,
		Payload:    payload,
	}, nil
}

type debeziumWidget struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// These three fixtures are byte-for-byte what a live Debezium 2.7.3.Final
// Postgres connector actually published for a real insert/update/delete
// against postgres-write (see local/docker-compose) — captured directly
// off the wire, not hand-written to match what the code expects.

const debeziumCreateFixture = `{"before":null,"after":{"id":"w1","name":"gadget","active":true},"source":{"version":"2.7.3.Final","connector":"postgresql","name":"cqrs","ts_ms":1790710562903,"snapshot":"false","db":"cqrs_write","sequence":"[\"306218576\",\"314477488\"]","ts_us":1790710562903499,"ts_ns":1790710562903499000,"schema":"public","table":"widgets","txId":65249,"lsn":314477488,"xmin":null},"transaction":null,"op":"c","ts_ms":1790710563391,"ts_us":1790710563391393,"ts_ns":1790710563391393044}`

const debeziumUpdateFixture = `{"before":null,"after":{"id":"w1","name":"gadget2","active":false},"source":{"version":"2.7.3.Final","connector":"postgresql","name":"cqrs","ts_ms":1790710563909,"snapshot":"false","db":"cqrs_write","sequence":"[\"314477768\",\"314477768\"]","ts_us":1790710563909780,"ts_ns":1790710563909780000,"schema":"public","table":"widgets","txId":65250,"lsn":314477768,"xmin":null},"transaction":null,"op":"u","ts_ms":1790710564417,"ts_us":1790710564417018,"ts_ns":1790710564417018711}`

const debeziumDeleteFixture = `{"before":{"id":"w1","name":"","active":false},"after":null,"source":{"version":"2.7.3.Final","connector":"postgresql","name":"cqrs","ts_ms":1790710564912,"snapshot":"false","db":"cqrs_write","sequence":"[\"314477896\",\"314477896\"]","ts_us":1790710564912478,"ts_ns":1790710564912478000,"schema":"public","table":"widgets","txId":65251,"lsn":314477896,"xmin":null},"transaction":null,"op":"d","ts_ms":1790710564925,"ts_us":1790710564925024,"ts_ns":1790710564925024837}`

func TestDebeziumTransform_HappyPath_RealCapturedFixtures(t *testing.T) {
	t.Parallel()
	t.Run("Create", func(t *testing.T) {
		got, err := transformDebezium[debeziumWidget]([]byte(debeziumCreateFixture))
		if err != nil {
			t.Fatalf("transformDebezium returned error: %v", err)
		}
		if got.ChangeType != domains.ChangeTypeCreated {
			t.Errorf("expected ChangeTypeCreated, got %v", got.ChangeType)
		}
		if got.Payload != (debeziumWidget{ID: "w1", Name: "gadget", Active: true}) {
			t.Errorf("unexpected payload: %+v", got.Payload)
		}
		if got.EventID != "widgets-314477488" {
			t.Errorf("expected EventID derived from table+lsn, got %q", got.EventID)
		}
	})

	t.Run("Update", func(t *testing.T) {
		got, err := transformDebezium[debeziumWidget]([]byte(debeziumUpdateFixture))
		if err != nil {
			t.Fatalf("transformDebezium returned error: %v", err)
		}
		if got.ChangeType != domains.ChangeTypeUpdated {
			t.Errorf("expected ChangeTypeUpdated, got %v", got.ChangeType)
		}
		if got.Payload != (debeziumWidget{ID: "w1", Name: "gadget2", Active: false}) {
			t.Errorf("unexpected payload: %+v", got.Payload)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		got, err := transformDebezium[debeziumWidget]([]byte(debeziumDeleteFixture))
		if err != nil {
			t.Fatalf("transformDebezium returned error: %v", err)
		}
		if got.ChangeType != domains.ChangeTypeDeleted {
			t.Errorf("expected ChangeTypeDeleted, got %v", got.ChangeType)
		}
		// Real, verified nuance: default REPLICA IDENTITY means "before" on
		// a delete only reliably carries the PK — Name comes back "" here,
		// not the row's actual last value. Fine for this library (deletes
		// only need the PK to remove the right row), but must not silently
		// regress into looking like full-fidelity "before" data.
		if got.Payload.ID != "w1" {
			t.Errorf("expected PK to survive, got %+v", got.Payload)
		}
	})
}

func TestDebeziumTransform_HappyPath_EventIDIsStableAndUnique(t *testing.T) {
	t.Parallel()
	a, err := transformDebezium[debeziumWidget]([]byte(debeziumCreateFixture))
	if err != nil {
		t.Fatalf("transformDebezium returned error: %v", err)
	}
	b, err := transformDebezium[debeziumWidget]([]byte(debeziumCreateFixture))
	if err != nil {
		t.Fatalf("transformDebezium returned error: %v", err)
	}
	if a.EventID != b.EventID {
		t.Errorf("expected the same input to produce the same EventID (stable, for dedup), got %q vs %q", a.EventID, b.EventID)
	}

	c, err := transformDebezium[debeziumWidget]([]byte(debeziumUpdateFixture))
	if err != nil {
		t.Fatalf("transformDebezium returned error: %v", err)
	}
	if a.EventID == c.EventID {
		t.Error("expected different LSNs to produce different EventIDs")
	}
}

func TestDebeziumTransform_SadPath_MalformedOrIncompleteInput(t *testing.T) {
	t.Parallel()
	t.Run("Malformed JSON", func(t *testing.T) {
		_, err := transformDebezium[debeziumWidget]([]byte("{not valid"))
		if err == nil {
			t.Fatal("expected an error for malformed JSON, got nil")
		}
	})

	t.Run("Unrecognized Op", func(t *testing.T) {
		_, err := transformDebezium[debeziumWidget]([]byte(`{"before":null,"after":{"id":"w1"},"op":"x","source":{"table":"widgets","lsn":1}}`))
		if err == nil {
			t.Fatal("expected an error for an unrecognized op, got nil")
		}
	})

	t.Run("Create With No After", func(t *testing.T) {
		_, err := transformDebezium[debeziumWidget]([]byte(`{"before":null,"after":null,"op":"c","source":{"table":"widgets","lsn":1}}`))
		// before==nil && after==nil is actually the tombstone case, so this
		// specific combination returns errDebeziumTombstone rather than a
		// generic error — verified below as its own case.
		if !errors.Is(err, errDebeziumTombstone) {
			t.Fatalf("expected errDebeziumTombstone for a fully-null record, got %v", err)
		}
	})

	t.Run("Update With No After But A Before Present", func(t *testing.T) {
		// Not realistic from a real connector, but the function must still
		// reject it explicitly rather than silently emit a zero-value
		// payload.
		_, err := transformDebezium[debeziumWidget]([]byte(`{"before":{"id":"w1"},"after":null,"op":"u","source":{"table":"widgets","lsn":1}}`))
		if err == nil {
			t.Fatal("expected an error for op=u with no 'after' payload, got nil")
		}
	})
}

func TestDebeziumTransform_PoisonPill_Tombstone(t *testing.T) {
	t.Parallel()
	_, err := transformDebezium[debeziumWidget]([]byte(`{"before":null,"after":null,"op":"","source":{"table":"widgets","lsn":1}}`))
	if !errors.Is(err, errDebeziumTombstone) {
		t.Fatalf("expected errDebeziumTombstone for a fully-null record, got %v", err)
	}
}
