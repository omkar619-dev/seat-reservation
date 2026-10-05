// Command server runs the seat reservation API.
//
// Boot order is built to survive cold starts: the HTTP listener comes up first (liveness
// is green immediately, readiness reports "starting"), then the database is pinged and
// migrated in the background with backoff. Business endpoints return 503 until that
// succeeds, so a slow or briefly unreachable database never crash-loops the service.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/omkar619-dev/seat-reservation/internal/auth"
	"github.com/omkar619-dev/seat-reservation/internal/booking"
	"github.com/omkar619-dev/seat-reservation/internal/config"
	"github.com/omkar619-dev/seat-reservation/internal/db"
	"github.com/omkar619-dev/seat-reservation/internal/httpapi"
	"github.com/omkar619-dev/seat-reservation/internal/obs"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" { // for Docker HEALTHCHECK (no curl in distroless)
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	// Every line goes to the in-memory tail (GET /logs); stdout is rate limited so the
	// platform's log pipeline samples predictably instead of dropping lines at random.
	logs := obs.NewRingBuffer(cfg.LogBufferLines)
	stdout := obs.NewRateLimitedWriter(os.Stdout, cfg.LogStdoutRate)
	logger := obs.NewLogger(io.MultiWriter(logs, stdout), cfg.LogLevel)
	slog.SetDefault(logger)
	metrics := obs.NewMetrics(version)
	metrics.Registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "log_stdout_lines_dropped_total",
		Help: "Info/debug log lines withheld from stdout by the rate limit (still in GET /logs).",
	}, func() float64 { return float64(stdout.Dropped()) }))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mainPool, err := db.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns, "seat-reservation")
	if err != nil {
		return err
	}
	defer mainPool.Close()
	// Small separate pool for readiness, metric scrapes and the auditor: request traffic can
	// saturate the main pool without making the service look dead or blinding its metrics.
	opsPool, err := db.NewPool(ctx, cfg.DatabaseURL, 3, "seat-reservation-ops")
	if err != nil {
		return err
	}
	defer opsPool.Close()
	metrics.RegisterDB(mainPool, opsPool, logger)

	store := booking.NewStore(mainPool, booking.Options{
		Precheck:  cfg.Precheck,
		OnTxRetry: func(code string) { metrics.TxRetries.WithLabelValues(code).Inc() },
	})
	ready := &httpapi.Readiness{}
	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: httpapi.New(httpapi.Config{
			Store:          store,
			Auth:           auth.New(cfg.JWTSecret, cfg.AdminKey, cfg.TokenTTL),
			Metrics:        metrics,
			Logs:           logs,
			PublicLogs:     cfg.PublicLogs,
			OpsPool:        opsPool,
			Ready:          ready,
			Logger:         logger,
			Version:        version,
			RequestTimeout: cfg.RequestTimeout,
			StartedAt:      time.Now(),
		}),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      cfg.RequestTimeout + 15*time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	go initDatabase(ctx, mainPool, logger, ready, metrics)
	go runAuditor(ctx, booking.NewStore(opsPool, booking.Options{}), opsPool, metrics, logger, ready,
		cfg.AuditInterval, cfg.AuditShows)

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("listening", slog.String("addr", srv.Addr), slog.String("version", version),
			slog.Bool("precheck", cfg.Precheck), slog.Int("db_max_conns", int(cfg.DBMaxConns)))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		stop() // restore default signal handling: a second Ctrl-C / SIGTERM now exits at once
	}

	// Graceful shutdown: fail readiness first so the load balancer stops routing new
	// traffic, keep serving stragglers for ShutdownDelay, then drain in-flight requests.
	logger.Info("shutdown: draining", slog.Duration("delay", cfg.ShutdownDelay))
	ready.SetDraining()
	metrics.Ready.Set(0)
	time.Sleep(cfg.ShutdownDelay)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("shutdown: forced", slog.String("error", err.Error()))
	}
	logger.Info("shutdown: complete")
	return nil
}

// initDatabase pings and migrates with capped exponential backoff until it succeeds.
func initDatabase(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, ready *httpapi.Readiness, metrics *obs.Metrics) {
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := func() error {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
			return db.Migrate(ctx, pool, logger)
		}()
		if err == nil {
			ready.SetInitialized()
			metrics.Ready.Set(1)
			logger.Info("database ready", slog.Int("attempt", attempt))
			return
		}
		logger.Warn("database not ready; retrying", slog.Int("attempt", attempt),
			slog.String("error", err.Error()), slog.Duration("retry_in", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

// runAuditor re-runs the cross-table reconciliation (the same checks as GET /shows/{id}/audit)
// for the most recent shows on a schedule and exports show_audit_ok, so an integrity
// violation pages someone instead of waiting for a human to ask. It runs on the ops pool, in
// a REPEATABLE READ snapshot per show, so it is exact even mid-burst.
func runAuditor(ctx context.Context, store *booking.Store, pool *pgxpool.Pool, metrics *obs.Metrics,
	logger *slog.Logger, ready *httpapi.Readiness, interval time.Duration, shows int) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !ready.Initialized() {
			continue
		}
		auditOnce(ctx, store, pool, metrics, logger, shows)
	}
}

func auditOnce(ctx context.Context, store *booking.Store, pool *pgxpool.Pool, metrics *obs.Metrics,
	logger *slog.Logger, shows int) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recent, err := booking.RecentShows(ctx, pool, shows)
	if err != nil {
		logger.Warn("auditor: listing shows failed", slog.String("error", err.Error()))
		return
	}
	metrics.ShowAuditOK.Reset() // only export shows still in the window
	complete := true
	for _, sh := range recent {
		rep, err := store.Audit(ctx, sh.ID)
		if err != nil {
			// "Couldn't check" is not "failed", but it must not look healthy either: count it, and
			// don't advance the last-run timestamp, so a staleness alert fires if it persists.
			complete = false
			metrics.AuditErrors.Inc()
			logger.Warn("auditor: audit did not run", slog.String("show_id", sh.ID), slog.String("error", err.Error()))
			continue
		}
		ok := 1.0
		if !rep.OK {
			ok = 0
			metrics.ReconcileFailures.Inc()
			logger.Error("RECONCILIATION FAILED", slog.String("show_id", sh.ID), slog.Any("checks", rep.Checks))
		}
		metrics.ShowAuditOK.WithLabelValues(sh.ID, sh.Name).Set(ok)
	}
	if complete {
		metrics.AuditLastRun.SetToCurrentTime()
	}
}

func healthcheck() int {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
