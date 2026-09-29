//go:build integration

// Chaos-adjacent tests: actually restart a real container mid-test and
// observe what happens, rather than assuming a client library's
// reconnect behavior works the way its docs say it should. Scoped to two
// concrete, high-value scenarios rather than an open-ended framework —
// Postgres restart (does database/sql's pool transparently recover?) and
// Kafka broker restart (does a real consumer group resume after its
// broker comes back?).
package regression

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/cqrs"

	segmentio "github.com/segmentio/kafka-go"
)

// restartContainer restarts a real docker container by name and waits for
// it to come back before returning, so the rest of the test isn't racing
// the container's own startup. Not every service in docker-compose.yml has
// a healthcheck (kafka and zookeeper don't) — this checks whether one is
// configured first, rather than assuming, and falls back to waiting for
// "running" plus a short grace period for containers that have none.
func restartContainer(t *testing.T, name string) {
	t.Helper()
	if out, err := exec.Command("docker", "restart", name).CombinedOutput(); err != nil {
		t.Fatalf("restarting container %s: %v (%s)", name, err, out)
	}

	hasHealthcheck := false
	if out, err := exec.Command("docker", "inspect", "--format", "{{if .State.Health}}yes{{end}}", name).CombinedOutput(); err == nil {
		hasHealthcheck = strings.TrimSpace(string(out)) == "yes"
	}

	if !hasHealthcheck {
		t.Logf("restarted container %s (no healthcheck configured), waiting for it to report running", name)
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			out, err := exec.Command("docker", "inspect", "--format", "{{.State.Status}}", name).CombinedOutput()
			if err == nil && strings.TrimSpace(string(out)) == "running" {
				time.Sleep(5 * time.Second) // grace period for the service inside to finish initializing
				return
			}
			time.Sleep(1 * time.Second)
		}
		t.Fatalf("container %s did not report running again within the timeout", name)
	}

	t.Logf("restarted container %s, waiting for it to report healthy", name)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", name).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("container %s did not report healthy again within the timeout", name)
}

// TestIntegration_Chaos_PostgresWriteRestartRecoversAutomatically restarts
// the real postgres-write container mid-test and confirms Create keeps
// working afterward without any special reconnection code on this
// library's part — verifying that database/sql's pool really does
// transparently dial a fresh connection once the server is back, for this
// stack's actual configuration (SetMaxOpenConns et al., see
// newPostgresSQLService), not just as a general Go behavior assumed to
// apply here too.
func TestIntegration_Chaos_PostgresWriteRestartRecoversAutomatically(t *testing.T) {
	skipUnlessInfraReachable(t)
	write := newPostgresSQLService(t, itWriteDSN)
	ctx := context.Background()

	c := cqrs.NewCQRS(cqrs.CQRSImpl[widget, widgetResource, any, string]{
		WriteSQLService: write,
		ToResource:      widgetToResource,
	})

	if _, err := c.Create(ctx, widget{ID: "before-restart", Name: "n"}); err != nil {
		t.Fatalf("Create before restart returned error: %v", err)
	}

	restartContainer(t, "cqrs-postgres-write")

	waitForCondition(t, 60*time.Second, func() bool {
		_, err := c.Create(ctx, widget{ID: "after-restart", Name: "n"})
		return err == nil
	}, "expected Create to eventually succeed again once postgres-write is back")

	// A single one-shot read right after restart can hit a connection the
	// pool hasn't finished recycling yet, even once pg_isready reports
	// healthy and even once some other query has already succeeded (with
	// SetMaxOpenConns(20) there are several distinct pooled connections,
	// and database/sql doesn't guarantee the very next one handed out is
	// the one already known-good) — so these reads get the same retry
	// treatment as the write above, rather than a single flaky attempt.
	waitForCondition(t, 15*time.Second, func() bool {
		_, ok := readWidgetFrom(t, write, "before-restart")
		return ok
	}, "expected the pre-restart row to have survived the restart (it's a real restart, not a data wipe)")
	waitForCondition(t, 15*time.Second, func() bool {
		_, ok := readWidgetFrom(t, write, "after-restart")
		return ok
	}, "expected the post-restart row to exist")
}

// TestIntegration_Chaos_KafkaRestartMidStream restarts the real kafka
// container while a real consumer group (realKafkaGroupBroker, not the
// simple no-group reader used elsewhere in this suite) is actively
// subscribed, and observes whether it resumes on its own. This is
// reporting real, observed behavior of this exact setup, not asserting
// what a client library's documentation promises in the abstract.
func TestIntegration_Chaos_KafkaRestartMidStream(t *testing.T) {
	skipUnlessInfraReachable(t)

	topic := fmt.Sprintf("cqrs-it-chaos-kafka-%d", time.Now().UnixNano())
	broker := newRealKafkaGroupBroker(itKafkaAddr, fmt.Sprintf("cqrs-it-chaos-kafka-group-%d", time.Now().UnixNano()), segmentio.FirstOffset)

	received := make(chan string, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subDone := make(chan error, 1)
	go func() {
		subDone <- broker.Subscribe(ctx, topic, func(key, value []byte) error {
			received <- string(value)
			return nil
		})
	}()
	time.Sleep(2 * time.Second) // let the consumer group join before publishing

	if err := broker.Publish(ctx, topic, []byte("k1"), []byte("before-restart")); err != nil {
		t.Fatalf("publishing before restart: %v", err)
	}
	select {
	case msg := <-received:
		if msg != "before-restart" {
			t.Fatalf("expected 'before-restart', got %q", msg)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the pre-restart message")
	}

	restartContainer(t, "cqrs-kafka")

	if err := broker.Publish(ctx, topic, []byte("k2"), []byte("after-restart")); err != nil {
		t.Fatalf("publishing after restart: %v", err)
	}
	select {
	case msg := <-received:
		if msg != "after-restart" {
			t.Fatalf("expected 'after-restart', got %q", msg)
		}
		t.Log("consumer group resumed on its own after the broker restart, no intervention needed")
	case <-time.After(30 * time.Second):
		t.Fatal("the consumer group did NOT resume on its own after the broker restart within 30s — " +
			"this is a real, observed finding: a long-lived Subscribe caller needs its own " +
			"reconnect/restart supervision around Run for this failure mode, not just kafka-go's " +
			"defaults")
	}

	cancel()
	select {
	case <-subDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}
