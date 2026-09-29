//go:build integration

package regression

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
)

// These tests answer "was it actually broadcasted?" for real: a real
// WebSocket client subscribes to the channel before anything happens, and
// the assertion is that a specific named event actually arrives over that
// live connection — not that BroadcastService.Broadcast was called
// in-process, which the fake-backed tests elsewhere already cover.

func TestIntegration_HappyPath_RealBroadcastDeliveredOverWebSocket_Created(t *testing.T) {
	c, _, _, broker, topic := newBroadcastIntegrationHarness(t, "bcast-created")
	sub := newPusherSubscriber(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	w := widget{ID: "w1", Name: "gadget", Active: true}
	if _, err := c.Create(ctx, w); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	envelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID: "evt-1", ChangeType: domains.ChangeTypeCreated, Payload: w,
	})
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(w.ID), envelope); err != nil {
		t.Fatalf("publishing to real kafka: %v", err)
	}

	msg := sub.waitForEvent(t, "widget.created", 15*time.Second)
	if msg.Channel != topic {
		t.Errorf("expected event on channel %q, got %q", topic, msg.Channel)
	}
	var got widgetResource
	if err := json.Unmarshal([]byte(msg.Data), &got); err != nil {
		t.Fatalf("decoding broadcast payload: %v", err)
	}
	if got.ID != "w1" || got.Name != "gadget" || !got.Active {
		t.Errorf("unexpected broadcast payload: %+v", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestIntegration_HappyPath_RealBroadcastDeliveredOverWebSocket_Updated(t *testing.T) {
	c, _, _, broker, topic := newBroadcastIntegrationHarness(t, "bcast-updated")
	sub := newPusherSubscriber(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	seed := widget{ID: "w1", Name: "old"}
	if _, err := c.Create(ctx, seed); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}
	seedEnvelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID: "evt-created", ChangeType: domains.ChangeTypeCreated, Payload: seed,
	})
	if err != nil {
		t.Fatalf("marshalling seed envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(seed.ID), seedEnvelope); err != nil {
		t.Fatalf("publishing seed to real kafka: %v", err)
	}
	sub.waitForEvent(t, "widget.created", 15*time.Second) // drain the seed broadcast

	updated := widget{ID: "w1", Name: "new"}
	if _, err := c.UpdateByID(ctx, "w1", updated); err != nil {
		t.Fatalf("UpdateByID returned error: %v", err)
	}
	updateEnvelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID: "evt-updated", ChangeType: domains.ChangeTypeUpdated, Payload: updated,
	})
	if err != nil {
		t.Fatalf("marshalling update envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(updated.ID), updateEnvelope); err != nil {
		t.Fatalf("publishing update to real kafka: %v", err)
	}

	msg := sub.waitForEvent(t, "widget.updated", 15*time.Second)
	var got widgetResource
	if err := json.Unmarshal([]byte(msg.Data), &got); err != nil {
		t.Fatalf("decoding broadcast payload: %v", err)
	}
	if got.Name != "new" {
		t.Errorf("expected broadcast payload name 'new', got %q", got.Name)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestIntegration_HappyPath_RealBroadcastDeliveredOverWebSocket_Deleted(t *testing.T) {
	c, _, _, broker, topic := newBroadcastIntegrationHarness(t, "bcast-deleted")
	sub := newPusherSubscriber(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	seed := widget{ID: "w1", Name: "n"}
	if _, err := c.Create(ctx, seed); err != nil {
		t.Fatalf("seed Create returned error: %v", err)
	}
	seedEnvelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID: "evt-created", ChangeType: domains.ChangeTypeCreated, Payload: seed,
	})
	if err != nil {
		t.Fatalf("marshalling seed envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(seed.ID), seedEnvelope); err != nil {
		t.Fatalf("publishing seed to real kafka: %v", err)
	}
	sub.waitForEvent(t, "widget.created", 15*time.Second) // drain the seed broadcast

	if err := c.DeleteByID(ctx, "w1"); err != nil {
		t.Fatalf("DeleteByID returned error: %v", err)
	}
	deleteEnvelope, err := sonic.Marshal(domains.CQRSQueuePayload[widget]{
		EventID: "evt-deleted", ChangeType: domains.ChangeTypeDeleted, Payload: seed,
	})
	if err != nil {
		t.Fatalf("marshalling delete envelope: %v", err)
	}
	if err := broker.Publish(ctx, topic, []byte(seed.ID), deleteEnvelope); err != nil {
		t.Fatalf("publishing delete to real kafka: %v", err)
	}

	msg := sub.waitForEvent(t, "widget.deleted", 15*time.Second)
	var got widgetResource
	if err := json.Unmarshal([]byte(msg.Data), &got); err != nil {
		t.Fatalf("decoding broadcast payload: %v", err)
	}
	if got.ID != "w1" {
		t.Errorf("expected broadcast payload id 'w1', got %q", got.ID)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
