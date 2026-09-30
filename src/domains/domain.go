package domains

import (
	"time"

	"github.com/uptrace/bun"
)

type (
	Channel    string
	Events     []string
	ChangeType int
)

const (
	ChangeTypeCreated ChangeType = iota
	ChangeTypeUpdated
	ChangeTypeDeleted
)

type CQRSQueuePayload[TData any] struct {
	EventID    string     `json:"event_id"`
	ChangeType ChangeType `json:"change_type"`
	Payload    TData      `json:"payload"`
}

type ProcessedEvent struct {
	bun.BaseModel `bun:"table:processed_events,alias:pe"`
	EventID       string    `bun:"event_id,pk"`
	Channel       string    `bun:"channel,notnull"`
	CreatedAt     time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}
