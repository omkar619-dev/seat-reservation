// Package obs holds observability plumbing: structured logging with request correlation,
// an in-memory log tail (public log access), and Prometheus metrics.
package obs

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
)

// NewLogger returns a JSON logger. slog's JSON handler emits each record with exactly one
// Write call, so every Write to w is one complete log line.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// RingBuffer keeps the last N log lines in memory so GET /logs can serve a live tail
// without depending on the hosting platform's log UI. Line i (0-based, counting every line
// ever written) lives at lines[i % N]; `written` doubles as a cursor for followers.
type RingBuffer struct {
	mu      sync.Mutex
	lines   [][]byte
	written uint64
}

func NewRingBuffer(n int) *RingBuffer {
	return &RingBuffer{lines: make([][]byte, max(n, 1))}
}

func (b *RingBuffer) Write(p []byte) (int, error) {
	line := bytes.TrimRight(p, "\n")
	cp := make([]byte, len(line)) // the logger reuses p's buffer
	copy(cp, line)
	b.mu.Lock()
	b.lines[b.written%uint64(len(b.lines))] = cp
	b.written++
	b.mu.Unlock()
	return len(p), nil
}

// Since returns the lines written after cursor that contain filter (oldest first, at most
// limit if limit > 0, keeping the newest), plus the cursor to pass next time. Lines already
// evicted from the buffer are skipped.
func (b *RingBuffer) Since(cursor uint64, filter string, limit int) ([][]byte, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	size := uint64(len(b.lines))
	start := cursor
	if b.written > size && start < b.written-size {
		start = b.written - size
	}
	var out [][]byte
	for i := start; i < b.written; i++ {
		if l := b.lines[i%size]; filter == "" || bytes.Contains(l, []byte(filter)) {
			out = append(out, l)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, b.written
}

// Request-scoped logging. The HTTP middleware creates one reqLog per request; handlers add
// attributes (user, show, outcome, reason) with Annotate, and the middleware writes them all
// in a single access-log line: one request = one line, correlated by request_id.

type reqLog struct {
	id     string
	logger *slog.Logger
	mu     sync.Mutex
	attrs  []slog.Attr
}

type reqLogKey struct{}

func WithRequest(ctx context.Context, base *slog.Logger, requestID string) context.Context {
	return context.WithValue(ctx, reqLogKey{}, &reqLog{
		id:     requestID,
		logger: base.With(slog.String("request_id", requestID)),
	})
}

// Logger returns the request's logger (carrying request_id), or the default logger.
func Logger(ctx context.Context) *slog.Logger {
	if rl, ok := ctx.Value(reqLogKey{}).(*reqLog); ok {
		return rl.logger
	}
	return slog.Default()
}

func RequestID(ctx context.Context) string {
	if rl, ok := ctx.Value(reqLogKey{}).(*reqLog); ok {
		return rl.id
	}
	return ""
}

// Annotate adds attributes to the request's access-log line.
func Annotate(ctx context.Context, attrs ...slog.Attr) {
	if rl, ok := ctx.Value(reqLogKey{}).(*reqLog); ok {
		rl.mu.Lock()
		rl.attrs = append(rl.attrs, attrs...)
		rl.mu.Unlock()
	}
}

// Annotations returns the attributes added with Annotate.
func Annotations(ctx context.Context) []slog.Attr {
	if rl, ok := ctx.Value(reqLogKey{}).(*reqLog); ok {
		rl.mu.Lock()
		defer rl.mu.Unlock()
		return append([]slog.Attr(nil), rl.attrs...)
	}
	return nil
}
