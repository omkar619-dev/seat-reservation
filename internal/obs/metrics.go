package obs

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/omkar619-dev/seat-reservation/internal/booking"
)

// Metrics are per-process counters plus gauges read from Postgres at scrape time.
//
// Counters answer "what happened" (confirmed, declined by reason, cancelled) and reset on
// restart, as Prometheus counters do. Seat gauges answer "what is true now" and are read
// from the database on every scrape, so they always agree with GET /shows/{id}.
type Metrics struct {
	Registry *prometheus.Registry

	ReservationsConfirmed prometheus.Counter
	SeatsConfirmed        prometheus.Counter
	ReservationsDeclined  *prometheus.CounterVec
	ReservationsCancelled prometheus.Counter
	SeatsReleased         prometheus.Counter
	TxRetries             *prometheus.CounterVec
	SpoofAttempts         prometheus.Counter
	ReconcileFailures     prometheus.Counter
	ShowAuditOK           *prometheus.GaugeVec
	AuditLastRun          prometheus.Gauge
	HTTPRequests          *prometheus.CounterVec
	HTTPDuration          *prometheus.HistogramVec
	InFlight              prometheus.Gauge
	Panics                prometheus.Counter
	Ready                 prometheus.Gauge
}

func NewMetrics(version string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	f := promauto.With(reg)
	m := &Metrics{
		Registry: reg,
		ReservationsConfirmed: f.NewCounter(prometheus.CounterOpts{
			Name: "reservations_confirmed_total",
			Help: "Reservations newly confirmed (idempotent replays excluded).",
		}),
		SeatsConfirmed: f.NewCounter(prometheus.CounterOpts{
			Name: "reservation_seats_confirmed_total",
			Help: "Seats confirmed by new reservations.",
		}),
		ReservationsDeclined: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_declined_total",
			Help: "Reserve requests that moved nothing, by reason and by the stage that decided " +
				"(precheck = read-only fast path, atomic = the transaction).",
		}, []string{"reason", "stage"}),
		ReservationsCancelled: f.NewCounter(prometheus.CounterOpts{
			Name: "reservations_cancelled_total",
			Help: "Reservations cancelled by their owner.",
		}),
		SeatsReleased: f.NewCounter(prometheus.CounterOpts{
			Name: "reservation_seats_released_total",
			Help: "Seats returned to available by cancellations.",
		}),
		TxRetries: f.NewCounterVec(prometheus.CounterOpts{
			Name: "db_tx_retries_total",
			Help: "Transactions retried after a deadlock, serialization failure or lock timeout (expected: 0).",
		}, []string{"sqlstate"}),
		SpoofAttempts: f.NewCounter(prometheus.CounterOpts{
			Name: "auth_spoof_attempts_total",
			Help: "Reserve requests whose body named a user_id different from the token (ignored).",
		}),
		ReconcileFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "reconciliation_failures_total",
			Help: "Audits (scheduled or GET /shows/{id}/audit) that found an invariant violation. Page on any increase.",
		}),
		ShowAuditOK: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "show_audit_ok",
			Help: "1 if the scheduled audit of the show passed every cross-table check, 0 if any failed. Page on 0.",
		}, []string{"show_id", "show_name"}),
		AuditLastRun: f.NewGauge(prometheus.GaugeOpts{
			Name: "show_audit_last_run_timestamp_seconds",
			Help: "Unix time the scheduled auditor last completed a cycle. Alert if it stops advancing.",
		}),
		HTTPRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by method and route pattern.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"method", "route"}),
		InFlight: f.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),
		Panics: f.NewCounter(prometheus.CounterOpts{
			Name: "http_panics_total",
			Help: "Recovered handler panics.",
		}),
		Ready: f.NewGauge(prometheus.GaugeOpts{
			Name: "app_ready",
			Help: "1 when the database is initialized and the instance is not draining.",
		}),
	}
	f.NewGauge(prometheus.GaugeOpts{
		Name:        "app_info",
		Help:        "Build information.",
		ConstLabels: prometheus.Labels{"version": version},
	}).Set(1)

	// Create series up front so dashboards and alerts see explicit zeros, not gaps.
	for _, code := range []string{"40P01", "40001", "55P03"} {
		m.TxRetries.WithLabelValues(code)
	}
	for _, reason := range []booking.DeclineReason{booking.ReasonSeatTaken, booking.ReasonPerUserLimit,
		booking.ReasonIdempotentReplay, booking.ReasonIdempotencyReuse} {
		for _, stage := range []booking.Stage{booking.StagePrecheck, booking.StageAtomic} {
			m.ReservationsDeclined.WithLabelValues(string(reason), string(stage))
		}
	}
	return m
}

func (m *Metrics) ObserveHTTP(method, route string, code int, d time.Duration) {
	m.HTTPRequests.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	m.HTTPDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

// RegisterDB adds collectors that read pool statistics (main pool) and seat counts (via the
// small ops pool, so a saturated main pool can never stall a scrape).
func (m *Metrics) RegisterDB(main, ops *pgxpool.Pool, log *slog.Logger) {
	m.Registry.MustRegister(newPoolCollector(main), newSeatCollector(ops, log))
}

type poolCollector struct {
	pool                                    *pgxpool.Pool
	max, total, acquired, idle              *prometheus.Desc
	acquires, emptyAcquires, canceled, wait *prometheus.Desc
}

func newPoolCollector(p *pgxpool.Pool) *poolCollector {
	d := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	return &poolCollector{
		pool:          p,
		max:           d("db_pool_max_conns", "Maximum connections in the main pool."),
		total:         d("db_pool_total_conns", "Open connections in the main pool."),
		acquired:      d("db_pool_acquired_conns", "Connections currently checked out."),
		idle:          d("db_pool_idle_conns", "Idle connections."),
		acquires:      d("db_pool_acquires_total", "Successful connection acquires."),
		emptyAcquires: d("db_pool_empty_acquires_total", "Acquires that had to wait because the pool was empty (saturation)."),
		canceled:      d("db_pool_canceled_acquires_total", "Acquires abandoned because the request context ended."),
		wait:          d("db_pool_acquire_seconds_total", "Total time spent acquiring connections."),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.max, c.total, c.acquired, c.idle, c.acquires, c.emptyAcquires, c.canceled, c.wait} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	g := func(d *prometheus.Desc, v float64) { ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v) }
	k := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	g(c.max, float64(s.MaxConns()))
	g(c.total, float64(s.TotalConns()))
	g(c.acquired, float64(s.AcquiredConns()))
	g(c.idle, float64(s.IdleConns()))
	k(c.acquires, float64(s.AcquireCount()))
	k(c.emptyAcquires, float64(s.EmptyAcquireCount()))
	k(c.canceled, float64(s.CanceledAcquireCount()))
	k(c.wait, s.AcquireDuration().Seconds())
}

// seatCollector exports seats_{available,held,confirmed,total} for the most recent shows,
// read from Postgres on every scrape.
type seatCollector struct {
	pool                                          *pgxpool.Pool
	log                                           *slog.Logger
	available, held, confirmed, total, reconciled *prometheus.Desc
	scrapeOK, dbUp                                *prometheus.Desc
}

const seatMetricShows = 50

func newSeatCollector(p *pgxpool.Pool, log *slog.Logger) *seatCollector {
	labels := []string{"show_id", "show_name"}
	return &seatCollector{
		pool:       p,
		log:        log,
		available:  prometheus.NewDesc("seats_available", "Seats currently available, per show.", labels, nil),
		held:       prometheus.NewDesc("seats_held", "Seats currently held, per show.", labels, nil),
		confirmed:  prometheus.NewDesc("seats_confirmed", "Seats currently confirmed, per show.", labels, nil),
		total:      prometheus.NewDesc("seats_total", "Total seats, per show.", labels, nil),
		reconciled: prometheus.NewDesc("seats_reconciled", "1 if available + held + confirmed == total_seats for the show.", labels, nil),
		scrapeOK:   prometheus.NewDesc("seat_metrics_scrape_success", "1 if seat gauges were read from the database on this scrape.", nil, nil),
		dbUp:       prometheus.NewDesc("db_up", "1 if Postgres answered a ping during this scrape (ops pool, 2s timeout). Page if 0 for 1m.", nil, nil),
	}
}

func (c *seatCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.available, c.held, c.confirmed, c.total, c.reconciled, c.scrapeOK, c.dbUp} {
		ch <- d
	}
}

func (c *seatCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// A dedicated ping, so db_up means "reachable", not "the seat query was slow".
	if err := c.pool.Ping(ctx); err != nil {
		c.log.Warn("metrics: database ping failed", slog.String("error", err.Error()))
		ch <- prometheus.MustNewConstMetric(c.dbUp, prometheus.GaugeValue, 0)
		ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.dbUp, prometheus.GaugeValue, 1)
	counts, err := booking.RecentShowCounts(ctx, c.pool, seatMetricShows)
	ok := 1.0
	if err != nil {
		ok = 0
		c.log.Warn("seat metrics scrape failed", slog.String("error", err.Error()))
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeOK, prometheus.GaugeValue, ok)
	for _, sc := range counts {
		g := func(d *prometheus.Desc, v int) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, float64(v), sc.ShowID, sc.Name)
		}
		g(c.available, sc.Available)
		g(c.held, sc.Held)
		g(c.confirmed, sc.Confirmed)
		g(c.total, sc.Total)
		reconciled := 0
		if sc.Available+sc.Held+sc.Confirmed == sc.Total {
			reconciled = 1
		}
		g(c.reconciled, reconciled)
	}
}
