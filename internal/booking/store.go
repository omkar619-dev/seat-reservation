package booking

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omkar619-dev/seat-reservation/internal/db"
)

const (
	DefaultPerUserLimit  = 4
	MaxPerUserLimit      = 100
	MaxSeatsPerShow      = 100_000
	MaxPricePaise        = 1_000_000_000_000 // keeps amount = price x seats far below int64 max
	MaxIdempotencyKeyLen = 200
	maxTxAttempts        = 3
)

var seatLabelRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,15}$`)

type Options struct {
	// Precheck enables the read-only fast path (see precheck). Correctness never depends on
	// it: the transaction makes the same decisions on its own. Tests run both ways.
	Precheck bool
	// OnTxRetry is called with the SQLSTATE each time a transaction is retried.
	OnTxRetry func(sqlstate string)
}

type Store struct {
	pool  *pgxpool.Pool
	shows *showCache
	opts  Options
}

func NewStore(pool *pgxpool.Pool, opts Options) *Store {
	if opts.OnTxRetry == nil {
		opts.OnTxRetry = func(string) {}
	}
	s := &Store{pool: pool, opts: opts}
	s.shows = newShowCache(s.loadShow)
	return s
}

// ---------------------------------------------------------------------------------------
// SQL. Each statement below is one atomic step; read them alongside reserveAtomic/Cancel.
// ---------------------------------------------------------------------------------------

const reservationCols = `id::text, show_id::text, user_id, request_hash, seat_labels,
	amount_paise, status, created_at, cancelled_at`

// Read-only fast path: one statement, so one snapshot for all three answers.
const precheckSQL = `
SELECT
    ARRAY(SELECT label FROM seats
          WHERE id = ANY($1::bigint[]) AND status <> 'available' ORDER BY id) AS unavailable,
    COALESCE((SELECT seats FROM show_user_seats
              WHERE show_id = $2::uuid AND user_id = $3), 0)            AS held,
    r.id::text, r.request_hash, r.seat_labels, r.amount_paise, r.status, r.created_at, r.cancelled_at
FROM (SELECT 1) AS one
LEFT JOIN reservations r
       ON r.show_id = $2::uuid AND r.user_id = $3 AND r.idempotency_key = $4`

// Step 1: claim the idempotency key. Same-key racers serialize on the unique index.
const insertReservationSQL = `
INSERT INTO reservations
    (id, show_id, user_id, idempotency_key, request_hash, seat_labels, amount_paise, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'confirmed')
ON CONFLICT (show_id, user_id, idempotency_key) DO NOTHING
RETURNING created_at`

// Step 2: claim seats. The MATERIALIZED CTE locks rows in ascending id order (ORDER BY is
// applied before FOR UPDATE), which is a global lock order, so overlapping multi-seat
// requests cannot deadlock. A racer that waited on a row lock re-evaluates
// status = 'available' against the winner's committed version and drops the row.
const claimSeatsSQL = `
WITH target AS MATERIALIZED (
    SELECT id FROM seats
    WHERE id = ANY($1::bigint[]) AND status = 'available'
    ORDER BY id
    FOR UPDATE
)
UPDATE seats AS s
SET status = 'confirmed', reservation_id = $2, user_id = $3, updated_at = now()
FROM target
WHERE s.id = target.id
RETURNING s.id`

// Step 3: per-user limit as a conditional upsert. ON CONFLICT DO UPDATE locks the user's
// counter row and evaluates the WHERE against its latest committed value, so parallel
// requests from one user are serialized here. No row returned = over the limit.
const bumpUserSeatsSQL = `
INSERT INTO show_user_seats AS u (show_id, user_id, seats)
SELECT $1::uuid, $2::text, $3::int WHERE $3::int <= $4::int
ON CONFLICT (show_id, user_id) DO UPDATE
    SET seats = u.seats + EXCLUDED.seats
    WHERE u.seats + EXCLUDED.seats <= $4::int
RETURNING seats`

// Cancel step 1: owner and current state are part of the guard, so only one of N concurrent
// cancels transitions the row, and a non-owner transitions nothing.
const cancelReservationSQL = `
UPDATE reservations
SET status = 'cancelled', cancelled_at = now()
WHERE id = $1 AND user_id = $2 AND status = 'confirmed'
RETURNING ` + reservationCols

// Cancel step 2: release only seats this reservation still owns (same ascending lock order).
const releaseSeatsSQL = `
WITH target AS MATERIALIZED (
    SELECT id FROM seats
    WHERE show_id = $1::uuid AND label = ANY($2::text[]) AND reservation_id = $3::uuid
    ORDER BY id
    FOR UPDATE
)
UPDATE seats AS s
SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
FROM target
WHERE s.id = target.id
RETURNING s.id`

// Cancel step 3: decrement by what was actually released.
const decrementUserSeatsSQL = `
UPDATE show_user_seats SET seats = seats - $3
WHERE show_id = $1::uuid AND user_id = $2`

// ---------------------------------------------------------------------------------------
// Shows
// ---------------------------------------------------------------------------------------

func (s *Store) CreateShow(ctx context.Context, in CreateShowInput) (*ShowState, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" || len(name) > 200 {
		return nil, invalid("name must be 1-200 characters")
	}
	if len(in.Seats) == 0 || len(in.Seats) > MaxSeatsPerShow {
		return nil, invalid("seats must contain between 1 and %d labels", MaxSeatsPerShow)
	}
	seen := make(map[string]struct{}, len(in.Seats))
	for _, l := range in.Seats {
		if !seatLabelRE.MatchString(l) {
			return nil, invalid("invalid seat label %q: use 1-16 letters, digits, '-' or '_'", l)
		}
		if _, dup := seen[l]; dup {
			return nil, invalid("duplicate seat label %q", l)
		}
		seen[l] = struct{}{}
	}
	if in.PricePaise < 0 || in.PricePaise > MaxPricePaise {
		return nil, invalid("price_paise must be an integer between 0 and %d", int64(MaxPricePaise))
	}
	limit := in.PerUserLimit
	if limit == 0 {
		limit = DefaultPerUserLimit
	}
	if limit < 1 || limit > MaxPerUserLimit {
		return nil, invalid("per_user_limit must be between 1 and %d", MaxPerUserLimit)
	}

	sh := &Show{
		ID:           uuid.Must(uuid.NewV7()).String(),
		Name:         name,
		PricePaise:   in.PricePaise,
		PerUserLimit: limit,
		TotalSeats:   len(in.Seats),
		labels:       slices.Clone(in.Seats),
		seatIDs:      make(map[string]int64, len(in.Seats)),
	}
	// Show and all its seats appear atomically: nobody can observe a half-created show.
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO shows (id, name, price_paise, per_user_limit, total_seats)
			 VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
			sh.ID, sh.Name, sh.PricePaise, sh.PerUserLimit, sh.TotalSeats,
		).Scan(&sh.CreatedAt); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`INSERT INTO seats (show_id, label, position)
			 SELECT $1::uuid, t.label, t.pos
			 FROM unnest($2::text[]) WITH ORDINALITY AS t(label, pos)
			 ORDER BY t.pos
			 RETURNING id, label`,
			sh.ID, sh.labels)
		if err != nil {
			return err
		}
		var id int64
		var label string
		_, err = pgx.ForEachRow(rows, []any{&id, &label}, func() error {
			sh.seatIDs[label] = id
			return nil
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create show: %w", err)
	}
	s.shows.put(sh)

	st := &ShowState{Show: *sh, Available: sh.TotalSeats, InvariantOK: true,
		Seats: make([]SeatState, len(sh.labels))}
	for i, l := range sh.labels {
		st.Seats[i] = SeatState{Label: l, Status: SeatAvailable}
	}
	return st, nil
}

// ShowState returns counts (and optionally every seat) from a single statement, i.e. a single
// snapshot, so the reconciliation invariant holds in every response, even mid-burst.
func (s *Store) ShowState(ctx context.Context, showID string, withSeats bool) (*ShowState, error) {
	sh, err := s.show(ctx, showID)
	if err != nil {
		return nil, err
	}
	st := &ShowState{Show: *sh}
	if !withSeats {
		err := s.pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'available'),
			       count(*) FILTER (WHERE status = 'held'),
			       count(*) FILTER (WHERE status = 'confirmed')
			FROM seats WHERE show_id = $1`, sh.ID,
		).Scan(&st.Available, &st.Held, &st.Confirmed)
		if err != nil {
			return nil, err
		}
	} else {
		rows, err := s.pool.Query(ctx,
			`SELECT label, status FROM seats WHERE show_id = $1 ORDER BY position`, sh.ID)
		if err != nil {
			return nil, err
		}
		st.Seats = make([]SeatState, 0, sh.TotalSeats)
		var label, status string
		_, err = pgx.ForEachRow(rows, []any{&label, &status}, func() error {
			switch SeatStatus(status) {
			case SeatAvailable:
				st.Available++
			case SeatHeld:
				st.Held++
			case SeatConfirmed:
				st.Confirmed++
			}
			st.Seats = append(st.Seats, SeatState{Label: label, Status: SeatStatus(status)})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	st.InvariantOK = st.Available+st.Held+st.Confirmed == st.TotalSeats
	return st, nil
}

func (s *Store) show(ctx context.Context, id string) (*Show, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, ErrShowNotFound
	}
	return s.shows.get(ctx, u.String())
}

func (s *Store) loadShow(ctx context.Context, id string) (*Show, error) {
	sh := &Show{ID: id}
	err := s.pool.QueryRow(ctx,
		`SELECT name, price_paise, per_user_limit, total_seats, created_at FROM shows WHERE id = $1`, id,
	).Scan(&sh.Name, &sh.PricePaise, &sh.PerUserLimit, &sh.TotalSeats, &sh.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrShowNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT id, label FROM seats WHERE show_id = $1 ORDER BY position`, id)
	if err != nil {
		return nil, err
	}
	sh.labels = make([]string, 0, sh.TotalSeats)
	sh.seatIDs = make(map[string]int64, sh.TotalSeats)
	var seatID int64
	var label string
	if _, err := pgx.ForEachRow(rows, []any{&seatID, &label}, func() error {
		sh.labels = append(sh.labels, label)
		sh.seatIDs[label] = seatID
		return nil
	}); err != nil {
		return nil, err
	}
	return sh, nil
}

// resolveSeats validates requested labels and returns seat ids and labels sorted by seat id.
// The sort gives (1) a canonical form for idempotency hashing ([A2,A1] == [A1,A2]) and
// (2) the global lock order that keeps multi-seat requests deadlock-free.
func (sh *Show) resolveSeats(requested []string) ([]int64, []string, error) {
	if len(requested) == 0 {
		return nil, nil, invalid("seats must not be empty")
	}
	if len(requested) > MaxPerUserLimit {
		return nil, nil, invalid("too many seats in one request (max %d)", MaxPerUserLimit)
	}
	type seat struct {
		id    int64
		label string
	}
	picked := make([]seat, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	var unknown []string
	for _, l := range requested {
		if _, dup := seen[l]; dup {
			return nil, nil, invalid("seat %q requested more than once", l)
		}
		seen[l] = struct{}{}
		id, ok := sh.seatIDs[l]
		if !ok {
			unknown = append(unknown, l)
			continue
		}
		picked = append(picked, seat{id, l})
	}
	if len(unknown) > 0 {
		return nil, nil, &ValidationError{
			Msg:          "unknown seat(s) for this show: " + strings.Join(unknown, ","),
			UnknownSeats: unknown,
		}
	}
	slices.SortFunc(picked, func(a, b seat) int { return cmp.Compare(a.id, b.id) })
	ids := make([]int64, len(picked))
	labels := make([]string, len(picked))
	for i, p := range picked {
		ids[i], labels[i] = p.id, p.label
	}
	return ids, labels, nil
}

// requestHash fingerprints what a request asks for. Same key + different hash = key reuse.
func requestHash(showID string, labels []string) string {
	h := sha256.New()
	h.Write([]byte(showID))
	for _, l := range labels {
		h.Write([]byte{0})
		h.Write([]byte(l))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---------------------------------------------------------------------------------------
// Reserve
// ---------------------------------------------------------------------------------------

// Reserve books all requested seats for req.UserID or none of them (all-or-nothing).
func (s *Store) Reserve(ctx context.Context, req ReserveRequest) (ReserveResult, error) {
	sh, err := s.show(ctx, req.ShowID)
	if err != nil {
		return ReserveResult{}, err
	}
	key := req.IdempotencyKey
	if key == "" || len(key) > MaxIdempotencyKeyLen {
		return ReserveResult{}, invalid("idempotency key must be 1-%d characters", MaxIdempotencyKeyLen)
	}
	ids, labels, err := sh.resolveSeats(req.Seats)
	if err != nil {
		return ReserveResult{}, err
	}
	hash := requestHash(sh.ID, labels)

	// A request for more seats than the limit can never succeed, whatever the user holds.
	if len(ids) > sh.PerUserLimit {
		return ReserveResult{}, &DeclineError{Reason: ReasonPerUserLimit, Stage: StagePrecheck,
			Limit: sh.PerUserLimit, Held: -1, Requested: len(ids)}
	}
	if s.opts.Precheck {
		res, decided, err := s.precheck(ctx, sh, req.UserID, key, ids, hash)
		if decided || err != nil {
			return res, err
		}
	}
	return s.reserveAtomic(ctx, sh, req.UserID, key, ids, labels, hash)
}

// precheck is a read-only fast path. It may replay or decline, but it never takes a seat.
//
// In an on-sale stampede most requests are losers that arrive after the winner committed.
// Answering them with one indexed read keeps them out of the write path entirely (no row
// locks, no WAL, no rolled-back inserts), which keeps the database fast for real races.
//
// Why "read, then decline" is safe when "read, then take" is not: a decline only asserts
// "at this snapshot the seat was not available", which is true. Taking a seat always goes
// through the conditional UPDATE in reserveAtomic.
//
// Idempotency stays exact: a reservation row and its seat updates commit atomically, so one
// snapshot sees both (-> replay) or neither (-> seats look free, so we fall through to the
// transaction, which waits on the unique index if a same-key request is in flight).
func (s *Store) precheck(ctx context.Context, sh *Show, userID, key string, ids []int64, hash string) (ReserveResult, bool, error) {
	var (
		unavailable []string
		held        int
		rID         *string
		rHash       *string
		rSeats      []string
		rAmount     *int64
		rStatus     *string
		rCreated    *time.Time
		rCancelled  *time.Time
	)
	err := s.pool.QueryRow(ctx, precheckSQL, ids, sh.ID, userID, key).Scan(
		&unavailable, &held, &rID, &rHash, &rSeats, &rAmount, &rStatus, &rCreated, &rCancelled)
	if err != nil {
		return ReserveResult{}, true, err
	}
	if rID != nil {
		if *rHash != hash {
			return ReserveResult{}, true, &DeclineError{Reason: ReasonIdempotencyReuse, Stage: StagePrecheck}
		}
		existing := Reservation{ID: *rID, ShowID: sh.ID, UserID: userID, Seats: rSeats,
			AmountPaise: *rAmount, Status: *rStatus, CreatedAt: *rCreated, CancelledAt: rCancelled}
		return ReserveResult{Reservation: existing, Replayed: true, Stage: StagePrecheck}, true, nil
	}
	if len(unavailable) > 0 {
		return ReserveResult{}, true, &DeclineError{Reason: ReasonSeatTaken, Stage: StagePrecheck,
			Unavailable: unavailable, Requested: len(ids)}
	}
	if held+len(ids) > sh.PerUserLimit {
		return ReserveResult{}, true, &DeclineError{Reason: ReasonPerUserLimit, Stage: StagePrecheck,
			Limit: sh.PerUserLimit, Held: held, Requested: len(ids)}
	}
	return ReserveResult{}, false, nil
}

var errKeyUsed = errors.New("idempotency key already used")

// reserveAtomic is the actual decision: one READ COMMITTED transaction, three conditional
// writes, always in the same order (reservation -> seats ascending -> user counter):
//
//  1. INSERT the reservation ON CONFLICT (show_id, user_id, idempotency_key) DO NOTHING.
//     The unique index is the idempotency lock: a concurrent request with the same key
//     blocks on it until we finish, then sees our committed row (replay) or proceeds.
//  2. Claim the seats with a conditional UPDATE (claimSeatsSQL). Fewer rows than requested
//     means some seat was taken: roll back everything (all-or-nothing).
//  3. Bump the per-user counter with a conditional upsert. No row means over the limit.
//
// Any decline rolls the whole transaction back, so nothing partial is ever visible.
func (s *Store) reserveAtomic(ctx context.Context, sh *Show, userID, key string, ids []int64, labels []string, hash string) (ReserveResult, error) {
	var res Reservation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		res = Reservation{
			ID:          uuid.Must(uuid.NewV7()).String(),
			ShowID:      sh.ID,
			UserID:      userID,
			Seats:       labels,
			AmountPaise: sh.PricePaise * int64(len(ids)),
			Status:      ReservationConfirmed,
			requestHash: hash,
		}

		// 1. Idempotency key.
		err := tx.QueryRow(ctx, insertReservationSQL,
			res.ID, sh.ID, userID, key, hash, labels, res.AmountPaise).Scan(&res.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return errKeyUsed
		}
		if err != nil {
			return err
		}

		// 2. Seats.
		rows, err := tx.Query(ctx, claimSeatsSQL, ids, res.ID, userID)
		if err != nil {
			return err
		}
		claimed, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return err
		}
		if len(claimed) != len(ids) {
			return &DeclineError{Reason: ReasonSeatTaken, Stage: StageAtomic,
				Unavailable: unclaimed(ids, labels, claimed), Requested: len(ids)}
		}

		// 3. Per-user limit.
		var total int
		err = tx.QueryRow(ctx, bumpUserSeatsSQL, sh.ID, userID, len(ids), sh.PerUserLimit).Scan(&total)
		if errors.Is(err, pgx.ErrNoRows) {
			return &DeclineError{Reason: ReasonPerUserLimit, Stage: StageAtomic,
				Limit: sh.PerUserLimit, Held: -1, Requested: len(ids)}
		}
		return err
	})

	switch {
	case err == nil:
		return ReserveResult{Reservation: res}, nil
	case errors.Is(err, errKeyUsed):
		// DO NOTHING only skips the insert once the conflicting row is committed, so the
		// original reservation is visible now: replay it, or reject the reuse.
		existing, err := s.reservationByKey(ctx, sh.ID, userID, key)
		if err != nil {
			return ReserveResult{}, err
		}
		if existing.requestHash != hash {
			return ReserveResult{}, &DeclineError{Reason: ReasonIdempotencyReuse, Stage: StageAtomic}
		}
		return ReserveResult{Reservation: existing, Replayed: true, Stage: StageAtomic}, nil
	default:
		return ReserveResult{}, err
	}
}

func unclaimed(ids []int64, labels []string, claimed []int64) []string {
	got := make(map[int64]struct{}, len(claimed))
	for _, id := range claimed {
		got[id] = struct{}{}
	}
	var out []string
	for i, id := range ids {
		if _, ok := got[id]; !ok {
			out = append(out, labels[i])
		}
	}
	return out
}

// inTx runs fn in a READ COMMITTED transaction and retries on deadlock, serialization
// failure or lock timeout. With a global lock order deadlocks should not happen at all; the
// retry is a safety net so that a transient conflict can never surface as a 5xx.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	for attempt := 1; ; attempt++ {
		err := pgx.BeginFunc(ctx, s.pool, fn)
		code := db.PgCode(err)
		if !retryable(code) || attempt == maxTxAttempts {
			return err
		}
		s.opts.OnTxRetry(code)
		backoff := time.Duration(attempt*attempt*(5+rand.IntN(20))) * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func retryable(sqlstate string) bool {
	switch sqlstate {
	case "40P01", // deadlock_detected
		"40001", // serialization_failure
		"55P03": // lock_not_available (lock_timeout)
		return true
	}
	return false
}

// ---------------------------------------------------------------------------------------
// Cancel and lookups
// ---------------------------------------------------------------------------------------

// Cancel releases a reservation's seats. Only the owner may cancel. Cancelling an already
// cancelled reservation is a no-op (changed=false), not an error.
//
// The release is scoped by reservation_id, so it can only touch seats this reservation still
// owns: it can never "resurrect" a seat that is now confirmed to someone else.
func (s *Store) Cancel(ctx context.Context, reservationID, userID string) (Reservation, bool, error) {
	if _, err := uuid.Parse(reservationID); err != nil {
		return Reservation{}, false, ErrReservationNotFound
	}
	var res Reservation
	var changed bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		changed = false
		r, err := scanReservation(tx.QueryRow(ctx, cancelReservationSQL, reservationID, userID))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // nothing transitioned; classified below
		}
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, releaseSeatsSQL, r.ShowID, r.Seats, r.ID)
		if err != nil {
			return err
		}
		released, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return err
		}
		if len(released) > 0 {
			if _, err := tx.Exec(ctx, decrementUserSeatsSQL, r.ShowID, r.UserID, len(released)); err != nil {
				return err
			}
		}
		res, changed = r, true
		return nil
	})
	if err != nil {
		return Reservation{}, false, err
	}
	if changed {
		return res, true, nil
	}
	existing, err := s.Reservation(ctx, reservationID)
	if err != nil {
		return Reservation{}, false, err
	}
	if existing.UserID != userID {
		return Reservation{}, false, ErrNotOwner
	}
	return existing, false, nil // already cancelled
}

func (s *Store) Reservation(ctx context.Context, id string) (Reservation, error) {
	if _, err := uuid.Parse(id); err != nil {
		return Reservation{}, ErrReservationNotFound
	}
	r, err := scanReservation(s.pool.QueryRow(ctx,
		`SELECT `+reservationCols+` FROM reservations WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrReservationNotFound
	}
	return r, err
}

func (s *Store) reservationByKey(ctx context.Context, showID, userID, key string) (Reservation, error) {
	return scanReservation(s.pool.QueryRow(ctx,
		`SELECT `+reservationCols+` FROM reservations
		 WHERE show_id = $1 AND user_id = $2 AND idempotency_key = $3`,
		showID, userID, key))
}

func scanReservation(row pgx.Row) (Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.ShowID, &r.UserID, &r.requestHash, &r.Seats,
		&r.AmountPaise, &r.Status, &r.CreatedAt, &r.CancelledAt)
	return r, err
}
