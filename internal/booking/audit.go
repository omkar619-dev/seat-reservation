package booking

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type AuditCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// AuditReport reconciles a show across seats, reservations and per-user counters.
type AuditReport struct {
	ShowID                string       `json:"show_id"`
	OK                    bool         `json:"ok"`
	TotalSeats            int          `json:"total_seats"`
	Available             int          `json:"available"`
	Held                  int          `json:"held"`
	Confirmed             int          `json:"confirmed"`
	ConfirmedReservations int          `json:"confirmed_reservations"`
	RevenuePaise          int64        `json:"revenue_paise"`
	Checks                []AuditCheck `json:"checks"`
	CheckedAt             time.Time    `json:"checked_at"`
}

func (r *AuditReport) check(name string, ok bool, format string, args ...any) {
	r.Checks = append(r.Checks, AuditCheck{Name: name, OK: ok, Detail: fmt.Sprintf(format, args...)})
}

// Audit runs every reconciliation check inside one REPEATABLE READ, read-only transaction:
// all queries see the same snapshot, so the report is exact even while a burst is running.
func (s *Store) Audit(ctx context.Context, showID string) (*AuditReport, error) {
	sh, err := s.show(ctx, showID)
	if err != nil {
		return nil, err
	}
	rep := &AuditReport{ShowID: sh.ID, TotalSeats: sh.TotalSeats, CheckedAt: time.Now().UTC()}
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

	err = pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		var rows int
		if err := tx.QueryRow(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE status = 'available'),
			       count(*) FILTER (WHERE status = 'held'),
			       count(*) FILTER (WHERE status = 'confirmed')
			FROM seats WHERE show_id = $1`, sh.ID,
		).Scan(&rows, &rep.Available, &rep.Held, &rep.Confirmed); err != nil {
			return err
		}
		sum := rep.Available + rep.Held + rep.Confirmed
		rep.check("seat_states_sum_to_total", sum == sh.TotalSeats && rows == sh.TotalSeats,
			"available %d + held %d + confirmed %d = %d; total_seats %d; seat rows %d",
			rep.Available, rep.Held, rep.Confirmed, sum, sh.TotalSeats, rows)

		var badOwner int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM seats s
			LEFT JOIN reservations r ON r.id = s.reservation_id
			WHERE s.show_id = $1 AND s.status <> 'available'
			  AND (r.id IS NULL OR r.status <> 'confirmed'
			       OR r.user_id <> s.user_id OR r.show_id <> s.show_id)`, sh.ID,
		).Scan(&badOwner); err != nil {
			return err
		}
		rep.check("taken_seats_owned_by_confirmed_reservation", badOwner == 0,
			"%d taken seat(s) without a matching confirmed reservation", badOwner)

		var mismatched int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM reservations r
			LEFT JOIN (SELECT reservation_id, count(*) AS n FROM seats
			           WHERE show_id = $1 AND reservation_id IS NOT NULL
			           GROUP BY reservation_id) s ON s.reservation_id = r.id
			WHERE r.show_id = $1
			  AND COALESCE(s.n, 0) <> CASE WHEN r.status = 'confirmed'
			                              THEN cardinality(r.seat_labels) ELSE 0 END`, sh.ID,
		).Scan(&mismatched); err != nil {
			return err
		}
		rep.check("reservations_own_exactly_their_seats", mismatched == 0,
			"%d reservation(s) whose seat rows disagree with their seat list", mismatched)

		var soldTwice int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM (
			    SELECT label FROM reservations, unnest(seat_labels) AS label
			    WHERE show_id = $1 AND status = 'confirmed'
			    GROUP BY label HAVING count(*) > 1) d`, sh.ID,
		).Scan(&soldTwice); err != nil {
			return err
		}
		rep.check("no_seat_sold_twice", soldTwice == 0,
			"%d seat label(s) appear in more than one confirmed reservation", soldTwice)

		var drift int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM
			    (SELECT user_id, count(*) AS n FROM seats
			     WHERE show_id = $1 AND status <> 'available' GROUP BY user_id) a
			FULL JOIN
			    (SELECT user_id, seats AS n FROM show_user_seats
			     WHERE show_id = $1 AND seats > 0) c USING (user_id)
			WHERE COALESCE(a.n, 0) <> COALESCE(c.n, 0)`, sh.ID,
		).Scan(&drift); err != nil {
			return err
		}
		rep.check("per_user_counters_match_seats", drift == 0,
			"%d user(s) whose limit counter disagrees with seats held", drift)

		var overLimit int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM (
			    SELECT user_id FROM seats WHERE show_id = $1 AND status <> 'available'
			    GROUP BY user_id HAVING count(*) > $2) o`, sh.ID, sh.PerUserLimit,
		).Scan(&overLimit); err != nil {
			return err
		}
		rep.check("no_user_over_limit", overLimit == 0,
			"%d user(s) above per_user_limit %d", overLimit, sh.PerUserLimit)

		var seatsInConfirmed int
		if err := tx.QueryRow(ctx, `
			SELECT count(*),
			       COALESCE(sum(amount_paise), 0)::bigint,
			       COALESCE(sum(cardinality(seat_labels)), 0)::bigint
			FROM reservations WHERE show_id = $1 AND status = 'confirmed'`, sh.ID,
		).Scan(&rep.ConfirmedReservations, &rep.RevenuePaise, &seatsInConfirmed); err != nil {
			return err
		}
		taken := rep.Held + rep.Confirmed
		rep.check("confirmed_reservations_cover_taken_seats", seatsInConfirmed == taken,
			"%d seat(s) across confirmed reservations; %d taken seat rows", seatsInConfirmed, taken)
		expected := int64(taken) * sh.PricePaise
		rep.check("revenue_matches_price", rep.RevenuePaise == expected,
			"revenue %d paise; expected %d x %d = %d paise", rep.RevenuePaise, taken, sh.PricePaise, expected)
		return nil
	})
	if err != nil {
		return nil, err
	}
	rep.OK = true
	for _, c := range rep.Checks {
		rep.OK = rep.OK && c.OK
	}
	return rep, nil
}

// ShowCounts is one row of the seat gauges exported on /metrics.
type ShowCounts struct {
	ShowID    string
	Name      string
	Total     int
	Available int
	Held      int
	Confirmed int
}

// Querier is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// RecentShowCounts returns seat counts for the most recent shows, read straight from the
// database at scrape time, so the gauges always agree with GET /shows/{id}.
func RecentShowCounts(ctx context.Context, q Querier, limit int) ([]ShowCounts, error) {
	rows, err := q.Query(ctx, `
		SELECT sh.id::text, sh.name, sh.total_seats,
		       count(*) FILTER (WHERE s.status = 'available'),
		       count(*) FILTER (WHERE s.status = 'held'),
		       count(*) FILTER (WHERE s.status = 'confirmed')
		FROM (SELECT id, name, total_seats FROM shows ORDER BY created_at DESC LIMIT $1) sh
		JOIN seats s ON s.show_id = sh.id
		GROUP BY sh.id, sh.name, sh.total_seats`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ShowCounts, error) {
		var c ShowCounts
		err := row.Scan(&c.ShowID, &c.Name, &c.Total, &c.Available, &c.Held, &c.Confirmed)
		return c, err
	})
}
