package booking

// Integration tests against a real Postgres (TEST_DATABASE_URL; `make test` starts one).
// Concurrency bugs only show up against the real database, so nothing here is mocked.
// Every test runs twice: with the read-only precheck enabled and disabled, to prove the
// atomic transaction is correct on its own.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omkar619-dev/seat-reservation/internal/db"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		ctx := context.Background()
		pool, err := db.NewPool(ctx, url, 80, "booking-tests")
		if err != nil {
			fmt.Fprintln(os.Stderr, "pool:", err)
			os.Exit(1)
		}
		if err := db.Migrate(ctx, pool, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
			fmt.Fprintln(os.Stderr, "migrate:", err)
			os.Exit(1)
		}
		testPool = pool
		defer pool.Close()
	}
	code := m.Run()
	os.Exit(code)
}

var ctx = context.Background()

func newStore(t *testing.T, precheck bool) *Store {
	t.Helper()
	if testPool == nil {
		t.Skip("TEST_DATABASE_URL not set; run `make test`")
	}
	return NewStore(testPool, Options{Precheck: precheck})
}

// bothModes runs fn with the precheck fast path disabled and enabled.
func bothModes(t *testing.T, fn func(t *testing.T, s *Store)) {
	for _, precheck := range []bool{false, true} {
		t.Run(fmt.Sprintf("precheck=%v", precheck), func(t *testing.T) {
			fn(t, newStore(t, precheck))
		})
	}
}

// race starts n goroutines, releases them through one gate so they collide, and waits.
func race(n int, fn func(i int)) {
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			fn(i)
		}()
	}
	close(gate)
	wg.Wait()
}

func seatLabels(prefix string, n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("%s%d", prefix, i+1)
	}
	return out
}

func createShow(t *testing.T, s *Store, seats []string, limit int) *ShowState {
	t.Helper()
	st, err := s.CreateShow(ctx, CreateShowInput{Name: t.Name(), Seats: seats, PricePaise: 25000, PerUserLimit: limit})
	if err != nil {
		t.Fatalf("create show: %v", err)
	}
	return st
}

func mustReserve(t *testing.T, s *Store, showID, user string, seats []string, key string) ReserveResult {
	t.Helper()
	res, err := s.Reserve(ctx, ReserveRequest{ShowID: showID, UserID: user, Seats: seats, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("reserve %v for %s: %v", seats, user, err)
	}
	return res
}

func state(t *testing.T, s *Store, showID string) *ShowState {
	t.Helper()
	st, err := s.ShowState(ctx, showID, true)
	if err != nil {
		t.Fatalf("show state: %v", err)
	}
	if !st.InvariantOK {
		t.Fatalf("invariant broken: %+v", st)
	}
	return st
}

func seatStatus(st *ShowState, label string) SeatStatus {
	for _, seat := range st.Seats {
		if seat.Label == label {
			return seat.Status
		}
	}
	return ""
}

func requireAuditOK(t *testing.T, s *Store, showID string) {
	t.Helper()
	rep, err := s.Audit(ctx, showID)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !rep.OK {
		t.Fatalf("audit failed: %+v", rep.Checks)
	}
}

func declineOf(err error) *DeclineError {
	var d *DeclineError
	if errors.As(err, &d) {
		return d
	}
	return nil
}

func TestHotSeatHasExactlyOneWinner(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 20), 4)
		const contenders = 300
		errs := make([]error, contenders)
		race(contenders, func(i int) {
			// Same key string for everyone: keys are scoped per user, so they must not collide.
			_, errs[i] = s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: fmt.Sprintf("user-%d", i),
				Seats: []string{"A12"}, IdempotencyKey: "key-1"})
		})
		winners := 0
		for i, err := range errs {
			if err == nil {
				winners++
				continue
			}
			if d := declineOf(err); d == nil || d.Reason != ReasonSeatTaken {
				t.Fatalf("contender %d: want seat_taken decline, got %v", i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("winners = %d, want exactly 1", winners)
		}
		st := state(t, s, show.ID)
		if st.Confirmed != 1 || st.Available != 19 || seatStatus(st, "A12") != SeatConfirmed {
			t.Fatalf("unexpected state: confirmed=%d available=%d", st.Confirmed, st.Available)
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestPerUserLimitHoldsUnderConcurrency(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 20), 4)
		errs := make([]error, 10)
		race(10, func(i int) {
			_, errs[i] = s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "greedy",
				Seats: []string{fmt.Sprintf("A%d", i+1)}, IdempotencyKey: fmt.Sprintf("k-%d", i)})
		})
		ok := 0
		for i, err := range errs {
			if err == nil {
				ok++
				continue
			}
			if d := declineOf(err); d == nil || d.Reason != ReasonPerUserLimit {
				t.Fatalf("request %d: want per_user_limit decline, got %v", i, err)
			}
		}
		if ok != 4 {
			t.Fatalf("successes = %d, want exactly 4", ok)
		}
		_, err := s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "greedy",
			Seats: []string{"A15"}, IdempotencyKey: "one-more"})
		if d := declineOf(err); d == nil || d.Reason != ReasonPerUserLimit {
			t.Fatalf("5th seat: want per_user_limit, got %v", err)
		}
		if st := state(t, s, show.ID); st.Confirmed != 4 {
			t.Fatalf("confirmed = %d, want 4", st.Confirmed)
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestIdempotentRetriesReserveExactlyOnce(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 10), 4)
		const n = 50
		results := make([]ReserveResult, n)
		errs := make([]error, n)
		race(n, func(i int) {
			results[i], errs[i] = s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "alice",
				Seats: []string{"A1", "A2"}, IdempotencyKey: "same-key"})
		})
		ids := map[string]bool{}
		fresh := 0
		for i := range n {
			if errs[i] != nil {
				t.Fatalf("request %d: %v", i, errs[i])
			}
			ids[results[i].Reservation.ID] = true
			if !results[i].Replayed {
				fresh++
			}
		}
		if len(ids) != 1 || fresh != 1 {
			t.Fatalf("distinct reservations = %d, fresh = %d; want 1 and 1", len(ids), fresh)
		}

		// Same key, same seats in a different order: the same request, so a replay.
		res := mustReserve(t, s, show.ID, "alice", []string{"A2", "A1"}, "same-key")
		if !res.Replayed || !ids[res.Reservation.ID] {
			t.Fatalf("reordered retry should replay the original reservation: %+v", res)
		}
		// Same key, different seats: rejected, and nothing moves.
		_, err := s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "alice",
			Seats: []string{"A3"}, IdempotencyKey: "same-key"})
		if d := declineOf(err); d == nil || d.Reason != ReasonIdempotencyReuse {
			t.Fatalf("want idempotency_key_reused, got %v", err)
		}
		st := state(t, s, show.ID)
		if st.Confirmed != 2 || seatStatus(st, "A3") != SeatAvailable {
			t.Fatalf("confirmed = %d (want 2), A3 = %s", st.Confirmed, seatStatus(st, "A3"))
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestOverlappingMultiSeatRequestsNeverDeadlockOrDoubleSell(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 8), 4)
		const n = 200
		results := make([]ReserveResult, n)
		errs := make([]error, n)
		race(n, func(i int) {
			r := rand.New(rand.NewPCG(uint64(i), 42))
			k := 2 + r.IntN(2) // 2 or 3 seats, in random order: [A5 A2] races [A2 A5]
			seats := make([]string, k)
			for j, p := range r.Perm(8)[:k] {
				seats[j] = fmt.Sprintf("A%d", p+1)
			}
			results[i], errs[i] = s.Reserve(ctx, ReserveRequest{ShowID: show.ID,
				UserID: fmt.Sprintf("u-%d", i), Seats: seats, IdempotencyKey: "k"})
		})
		owner := map[string]string{}
		for i := range n {
			if errs[i] != nil {
				if d := declineOf(errs[i]); d == nil || d.Reason != ReasonSeatTaken {
					t.Fatalf("request %d: want seat_taken decline, got %v", i, errs[i])
				}
				continue
			}
			for _, seat := range results[i].Reservation.Seats {
				if prev, taken := owner[seat]; taken {
					t.Fatalf("seat %s sold twice: %s and %s", seat, prev, results[i].Reservation.ID)
				}
				owner[seat] = results[i].Reservation.ID
			}
		}
		if st := state(t, s, show.ID); st.Confirmed != len(owner) {
			t.Fatalf("confirmed = %d, but winners hold %d seats", st.Confirmed, len(owner))
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestPartialRequestIsAllOrNothing(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 5), 4)
		mustReserve(t, s, show.ID, "bob", []string{"A2"}, "k1")
		_, err := s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "carol",
			Seats: []string{"A1", "A2"}, IdempotencyKey: "k1"})
		d := declineOf(err)
		if d == nil || d.Reason != ReasonSeatTaken || !slices.Equal(d.Unavailable, []string{"A2"}) {
			t.Fatalf("want seat_taken for [A2], got %v", err)
		}
		if st := state(t, s, show.ID); seatStatus(st, "A1") != SeatAvailable {
			t.Fatalf("A1 must stay available after a failed [A1 A2] request")
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestCancelIsOwnerOnlyAndSeatsBecomeRebookable(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 5), 4)
		res := mustReserve(t, s, show.ID, "dave", []string{"A1", "A2"}, "k1")

		if _, _, err := s.Cancel(ctx, res.Reservation.ID, "mallory"); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("non-owner cancel: want ErrNotOwner, got %v", err)
		}
		if st := state(t, s, show.ID); st.Confirmed != 2 {
			t.Fatalf("non-owner cancel must not release seats")
		}

		r, changed, err := s.Cancel(ctx, res.Reservation.ID, "dave")
		if err != nil || !changed || r.Status != ReservationCancelled {
			t.Fatalf("owner cancel: changed=%v status=%s err=%v", changed, r.Status, err)
		}
		if _, changed, err := s.Cancel(ctx, res.Reservation.ID, "dave"); err != nil || changed {
			t.Fatalf("second cancel should be a no-op: changed=%v err=%v", changed, err)
		}
		if st := state(t, s, show.ID); st.Available != 5 {
			t.Fatalf("available = %d after cancel, want 5", st.Available)
		}

		mustReserve(t, s, show.ID, "erin", []string{"A1"}, "k1") // released seat is re-bookable

		// dave's original key replays the cancelled reservation; it does not book again.
		replay := mustReserve(t, s, show.ID, "dave", []string{"A1", "A2"}, "k1")
		if !replay.Replayed || replay.Reservation.Status != ReservationCancelled {
			t.Fatalf("want replay of cancelled reservation, got %+v", replay)
		}
		// dave's limit counter was released along with the seats.
		mustReserve(t, s, show.ID, "dave", []string{"A2", "A3", "A4", "A5"}, "k2")
		requireAuditOK(t, s, show.ID)
	})
}

func TestConcurrentCancelsReleaseExactlyOnce(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 4), 4)
		res := mustReserve(t, s, show.ID, "frank", []string{"A1", "A2", "A3"}, "k1")
		const n = 20
		changed := make([]bool, n)
		errs := make([]error, n)
		race(n, func(i int) {
			_, changed[i], errs[i] = s.Cancel(ctx, res.Reservation.ID, "frank")
		})
		transitions := 0
		for i := range n {
			if errs[i] != nil {
				t.Fatalf("cancel %d: %v", i, errs[i])
			}
			if changed[i] {
				transitions++
			}
		}
		if transitions != 1 {
			t.Fatalf("transitions = %d, want exactly 1", transitions)
		}
		if st := state(t, s, show.ID); st.Available != 4 {
			t.Fatalf("available = %d, want 4", st.Available)
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestCancelRacingReservesNeverDoubleSells(t *testing.T) {
	bothModes(t, func(t *testing.T, s *Store) {
		show := createShow(t, s, seatLabels("A", 3), 4)
		orig := mustReserve(t, s, show.ID, "orig", []string{"A1"}, "k")
		const n = 50
		errs := make([]error, n+1)
		race(n+1, func(i int) {
			if i == n {
				_, _, errs[i] = s.Cancel(ctx, orig.Reservation.ID, "orig")
				return
			}
			_, errs[i] = s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: fmt.Sprintf("r-%d", i),
				Seats: []string{"A1"}, IdempotencyKey: "k"})
		})
		if errs[n] != nil {
			t.Fatalf("cancel: %v", errs[n])
		}
		winners := 0
		for i := range n {
			if errs[i] == nil {
				winners++
			} else if d := declineOf(errs[i]); d == nil || d.Reason != ReasonSeatTaken {
				t.Fatalf("reserve %d: %v", i, errs[i])
			}
		}
		if winners > 1 {
			t.Fatalf("A1 re-sold %d times", winners)
		}
		if st := state(t, s, show.ID); st.Confirmed != winners {
			t.Fatalf("confirmed = %d, winners = %d", st.Confirmed, winners)
		}
		requireAuditOK(t, s, show.ID)
	})
}

func TestReserveValidation(t *testing.T) {
	s := newStore(t, true)
	show := createShow(t, s, seatLabels("A", 5), 2)
	req := func(seats []string, key string) error {
		_, err := s.Reserve(ctx, ReserveRequest{ShowID: show.ID, UserID: "v", Seats: seats, IdempotencyKey: key})
		return err
	}
	var ve *ValidationError
	if err := req([]string{"Z9"}, "k"); !errors.As(err, &ve) || !slices.Equal(ve.UnknownSeats, []string{"Z9"}) {
		t.Fatalf("unknown seat: %v", err)
	}
	if err := req([]string{"A1", "A1"}, "k"); !errors.As(err, &ve) {
		t.Fatalf("duplicate seat: %v", err)
	}
	if err := req(nil, "k"); !errors.As(err, &ve) {
		t.Fatalf("empty seats: %v", err)
	}
	if err := req([]string{"A1"}, ""); !errors.As(err, &ve) {
		t.Fatalf("missing key: %v", err)
	}
	if d := declineOf(req([]string{"A1", "A2", "A3"}, "k")); d == nil || d.Reason != ReasonPerUserLimit {
		t.Fatalf("3 seats with limit 2 should be a per_user_limit decline")
	}
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
		_, err := s.Reserve(ctx, ReserveRequest{ShowID: id, UserID: "v", Seats: []string{"A1"}, IdempotencyKey: "k"})
		if !errors.Is(err, ErrShowNotFound) {
			t.Fatalf("show %q: want ErrShowNotFound, got %v", id, err)
		}
	}
	requireAuditOK(t, s, show.ID)
}
