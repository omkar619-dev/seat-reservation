// Package booking is the system of record for shows, seats and reservations.
//
// Every decision that must be race-free (who gets a seat, whether a user is over their
// limit, whether an idempotency key was already used) is made by a single atomic step in
// Postgres. Go code never decides "the seat is free, so take it" from something it read
// earlier. See store.go for the exact statements.
package booking

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type SeatStatus string

const (
	SeatAvailable SeatStatus = "available"
	SeatHeld      SeatStatus = "held" // reserved for the time-boxed hold model; unused by the confirm-on-reserve flow
	SeatConfirmed SeatStatus = "confirmed"
)

const (
	ReservationConfirmed = "confirmed"
	ReservationCancelled = "cancelled"
)

// Show is immutable after creation, which is what makes caching it per-instance safe.
type Show struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	PricePaise   int64     `json:"price_paise"`
	PerUserLimit int       `json:"per_user_limit"`
	TotalSeats   int       `json:"total_seats"`
	CreatedAt    time.Time `json:"created_at"`

	labels  []string         // seat labels in position order
	seatIDs map[string]int64 // label -> seats.id
}

type SeatState struct {
	Label  string     `json:"label"`
	Status SeatStatus `json:"status"`
}

// ShowState is a point-in-time view of a show. All counts and per-seat statuses come from a
// single SQL statement (one snapshot), so available+held+confirmed == total_seats always.
type ShowState struct {
	Show
	Available   int         `json:"available"`
	Held        int         `json:"held"`
	Confirmed   int         `json:"confirmed"`
	InvariantOK bool        `json:"invariant_ok"`
	Seats       []SeatState `json:"seats,omitempty"`
}

type Reservation struct {
	ID          string     `json:"reservation_id"`
	ShowID      string     `json:"show_id"`
	UserID      string     `json:"user_id"`
	Seats       []string   `json:"seats"`
	AmountPaise int64      `json:"amount_paise"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`

	requestHash string
}

// ReserveRequest is built by the HTTP layer. UserID always comes from the verified token.
type ReserveRequest struct {
	ShowID         string
	UserID         string
	Seats          []string
	IdempotencyKey string
}

// ReserveResult is a successful outcome: either a brand-new reservation or a replay of the
// reservation previously created with the same idempotency key.
type ReserveResult struct {
	Reservation Reservation
	Replayed    bool
	Stage       Stage // where a replay was detected
}

type CreateShowInput struct {
	Name         string
	Seats        []string
	PricePaise   int64
	PerUserLimit int // 0 means DefaultPerUserLimit
}

var (
	ErrShowNotFound        = errors.New("show not found")
	ErrReservationNotFound = errors.New("reservation not found")
	ErrNotOwner            = errors.New("reservation belongs to another user")
)

// ValidationError means the request itself is malformed (HTTP 400).
type ValidationError struct {
	Msg          string
	UnknownSeats []string
}

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

// DeclineReason is a clean, expected "no" (HTTP 409), never a server error.
type DeclineReason string

const (
	ReasonSeatTaken        DeclineReason = "seat_taken"
	ReasonPerUserLimit     DeclineReason = "per_user_limit"
	ReasonIdempotencyReuse DeclineReason = "idempotency_key_reused"
	// ReasonIdempotentReplay is not a DeclineError (a replay returns the original reservation),
	// but it is counted with the declines because the request moved nothing.
	ReasonIdempotentReplay DeclineReason = "idempotent_replay"
)

// Stage records which step produced an outcome:
//   - precheck: a read-only fast path that can only ever decline or replay, never take a seat.
//   - atomic:   the transaction whose conditional writes are the real decision.
type Stage string

const (
	StagePrecheck Stage = "precheck"
	StageAtomic   Stage = "atomic"
)

type DeclineError struct {
	Reason      DeclineReason
	Stage       Stage
	Unavailable []string // seat_taken: requested seats that were not available
	Limit       int      // per_user_limit
	Held        int      // per_user_limit: seats the user already holds (-1 if unknown)
	Requested   int
}

func (e *DeclineError) Error() string {
	switch e.Reason {
	case ReasonSeatTaken:
		return "seat(s) not available: " + strings.Join(e.Unavailable, ",")
	case ReasonPerUserLimit:
		return fmt.Sprintf("per-user limit of %d seats for this show would be exceeded", e.Limit)
	case ReasonIdempotencyReuse:
		return "idempotency key was already used with a different request"
	default:
		return string(e.Reason)
	}
}
