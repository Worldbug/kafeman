package models

import (
	"io"
	"time"
)

type ConsumeCommand struct {
	Topic           string
	ConsumerGroup   string
	Partitions      []int32
	CommitMessages  bool
	Offset          int64
	Follow          bool
	WithMeta        bool
	MessagesCount   int32
	LimitedMessages bool
	FromTime        time.Time
	Output          io.Writer
}
