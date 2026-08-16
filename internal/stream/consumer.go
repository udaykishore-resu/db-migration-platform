// Package stream consumes the change stream from Kafka.
//
// One design decision dominates this package: the platform manages its own
// offsets and never uses Kafka's consumer-group offset commit. The broker's
// committed offset and the target database's contents cannot be updated
// atomically, so any design that relies on the broker's offset has a window in
// which they disagree — and that window is exactly where duplicated or skipped
// batches come from. Here the offset lives in the target database, written in the
// same transaction as the rows, and the reader is positioned from it on startup.
package stream

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/udaykishore-resu/db-migration-platform/internal/config"
	"github.com/udaykishore-resu/db-migration-platform/internal/sink"
)

// Message is one raw record from the stream.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}

// Consumer reads a batch of messages at a time.
type Consumer struct {
	readers []*kafka.Reader
	batch   int
	wait    time.Duration
}

// NewConsumer builds a reader per topic-partition assignment.
//
// Readers are created per partition rather than using a consumer group, because
// a group assigns partitions dynamically and rebalances at times the platform
// does not control — and a rebalance in the middle of an apply transaction is
// precisely when the offset and the data are most likely to disagree. Explicit
// partition assignment makes the mapping from partition to offset row stable.
func NewConsumer(cfg config.Kafka, assignments map[string][]int32, start []sink.Position) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("stream: no Kafka brokers configured")
	}

	resume := make(map[string]int64, len(start))
	for _, p := range start {
		resume[fmt.Sprintf("%s/%d", p.Topic, p.Partition)] = p.Offset
	}

	var dialer *kafka.Dialer
	if cfg.TLS {
		dialer = &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
			TLS:       &tls.Config{MinVersion: tls.VersionTLS12},
		}
	}

	c := &Consumer{
		batch: 1000,
		wait:  config.Duration(cfg.MaxWait, 500*time.Millisecond),
	}

	for topic, partitions := range assignments {
		for _, p := range partitions {
			rc := kafka.ReaderConfig{
				Brokers:   cfg.Brokers,
				Topic:     topic,
				Partition: int(p),
				MinBytes:  orDefault(cfg.MinBytes, 1<<10),
				MaxBytes:  orDefault(cfg.MaxBytes, 10<<20),
				MaxWait:   c.wait,
				Dialer:    dialer,
			}
			r := kafka.NewReader(rc)

			// The stored offset always wins: it is the one written atomically
			// with the data. Only when there is none does the configured start
			// position apply.
			if off, ok := resume[fmt.Sprintf("%s/%d", topic, p)]; ok {
				if err := r.SetOffset(off + 1); err != nil {
					return nil, fmt.Errorf("stream: seeking %s/%d to %d: %w", topic, p, off+1, err)
				}
			} else if cfg.StartOffset == "latest" {
				if err := r.SetOffset(kafka.LastOffset); err != nil {
					return nil, fmt.Errorf("stream: seeking %s/%d to latest: %w", topic, p, err)
				}
			} else if err := r.SetOffset(kafka.FirstOffset); err != nil {
				return nil, fmt.Errorf("stream: seeking %s/%d to earliest: %w", topic, p, err)
			}

			c.readers = append(c.readers, r)
		}
	}
	if len(c.readers) == 0 {
		return nil, fmt.Errorf("stream: no partitions assigned")
	}
	return c, nil
}

// SetBatchSize bounds how many messages one Poll returns.
func (c *Consumer) SetBatchSize(n int) {
	if n > 0 {
		c.batch = n
	}
}

// Poll reads up to one batch, returning early when the linger window expires.
//
// The linger is what keeps a low-traffic stream from committing one transaction
// per message while still letting a high-traffic stream fill whole batches. Both
// extremes are pathological: per-message transactions make the target's WAL the
// bottleneck, and waiting indefinitely for a full batch makes lag unbounded when
// traffic is light.
func (c *Consumer) Poll(ctx context.Context) ([]Message, error) {
	deadline := time.Now().Add(c.wait)
	out := make([]Message, 0, c.batch)

	for _, r := range c.readers {
		for len(out) < c.batch && time.Now().Before(deadline) {
			readCtx, cancel := context.WithDeadline(ctx, deadline)
			m, err := r.ReadMessage(readCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return out, ctx.Err()
				}
				// A deadline here means this partition simply had nothing more
				// to give within the linger window, which is normal.
				break
			}
			out = append(out, Message{
				Topic:     m.Topic,
				Partition: int32(m.Partition), //nolint:gosec // partition counts are small
				Offset:    m.Offset,
				Key:       m.Key,
				Value:     m.Value,
				Time:      m.Time,
			})
		}
	}
	return out, nil
}

// Lag reports the difference between the newest available offset and the last
// one read, per partition.
func (c *Consumer) Lag(ctx context.Context) (map[string]int64, error) {
	out := make(map[string]int64, len(c.readers))
	for _, r := range c.readers {
		stats := r.Stats()
		out[fmt.Sprintf("%s/%s", stats.Topic, stats.Partition)] = stats.Lag
	}
	_ = ctx
	return out, nil
}

// Close shuts every reader down.
func (c *Consumer) Close() error {
	var firstErr error
	for _, r := range c.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// DiscoverPartitions asks the brokers which partitions each topic has, so the
// service does not need them listed in configuration.
func DiscoverPartitions(ctx context.Context, brokers []string, topics []string) (map[string][]int32, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("stream: no brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("stream: dialling %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	parts, err := conn.ReadPartitions(topics...)
	if err != nil {
		return nil, fmt.Errorf("stream: reading partition metadata: %w", err)
	}

	out := make(map[string][]int32, len(topics))
	for _, p := range parts {
		out[p.Topic] = append(out[p.Topic], int32(p.ID)) //nolint:gosec // partition ids are small
	}
	return out, nil
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
