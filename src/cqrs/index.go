package cqrs

import (
	"time"

	"github.com/Lands-Horizon-Corp/cqrs-go/src/domains"
	"github.com/Lands-Horizon-Corp/cqrs-go/src/utils"
	"github.com/go-playground/validator/v10"
)

type CQRSImpl[TData any, TResponse any, TRequest any, TID any] struct {
	// Default column names for ID and sorting
	Channel           domains.Channel
	ColumnDefaultID   string
	ColumnDefaultSort string
	Preloads          []string

	// Conversion functions for transforming data into different representations
	ToResource func(*TData) *TResponse
	TocCSV     func(*TData) *map[string]any

	// Callbacks
	Created  func(*TData) domains.Events
	Updated  func(*TData) domains.Events
	Deleted  func(*TData) domains.Events
	Dispatch func(channel domains.Channel, events domains.Events, payload *TResponse) error

	// Services
	ReadSQLService       domains.SQLService
	WriteSQLService      domains.SQLService
	LogService           domains.LogService
	BroadcastService     domains.BroadcastService
	MessageBrokerService domains.MessageBrokerService

	// Validator for struct validation
	Validator *validator.Validate

	// Pools for efficient memory management of commonly used data structures
	stringSlicePool     *utils.BufferPool[string]
	stringSetPool       *utils.MapPool[string, bool]
	processedEventsPool *utils.BufferPool[domains.ProcessedEvent]
	batchSize           int
	flushInterval       time.Duration

	// idFieldIndex is the struct field index on TData whose `bun` tag
	// names ColumnDefaultID, resolved once here instead of on every
	// message in the hot batching path. -1 means no matching field.
	idFieldIndex int
}

func NewCQRS[TData any, TResponse any, TRequest any, TID any](
	c CQRSImpl[TData, TResponse, TRequest, TID],
) *CQRSImpl[TData, TResponse, TRequest, TID] {
	if c.ColumnDefaultID == "" {
		c.ColumnDefaultID = "id"
	}
	if c.ColumnDefaultSort == "" {
		c.ColumnDefaultSort = "updated_at DESC"
	}
	if c.Channel == "" {
		c.Channel = "default"
	}
	if c.Validator == nil {
		c.Validator = validator.New()
	}
	if c.WriteSQLService == nil {
		panic("WriteSQLService must be initialized")
	}
	if c.batchSize == 0 {
		c.batchSize = 100
	}
	if c.flushInterval == 0 {
		c.flushInterval = 5 * time.Second
	}
	return &CQRSImpl[TData, TResponse, TRequest, TID]{
		ColumnDefaultID:      c.ColumnDefaultID,
		ColumnDefaultSort:    c.ColumnDefaultSort,
		Preloads:             c.Preloads,
		ToResource:           c.ToResource,
		TocCSV:               c.TocCSV,
		Created:              c.Created,
		Updated:              c.Updated,
		Deleted:              c.Deleted,
		Dispatch:             c.Dispatch,
		ReadSQLService:       c.ReadSQLService,
		WriteSQLService:      c.WriteSQLService,
		LogService:           c.LogService,
		BroadcastService:     c.BroadcastService,
		MessageBrokerService: c.MessageBrokerService,
		Validator:            c.Validator,
		stringSlicePool:      utils.NewBufferPool[string](),
		stringSetPool:        utils.NewMapPool[string, bool](),
		processedEventsPool:  utils.NewBufferPool[domains.ProcessedEvent](),
		batchSize:            c.batchSize,
		flushInterval:        c.flushInterval,
		idFieldIndex:         utils.BunColumnFieldIndex[TData](c.ColumnDefaultID),
	}
}
