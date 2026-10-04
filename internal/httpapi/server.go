// Package httpapi is the JSON HTTP interface. It translates HTTP to booking calls and
// booking outcomes to status codes, metrics and one access-log line per request.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/omkar619-dev/seat-reservation/internal/auth"
	"github.com/omkar619-dev/seat-reservation/internal/booking"
	"github.com/omkar619-dev/seat-reservation/internal/obs"
)

// Readiness tracks lifecycle: "initialized" once the database is reachable and migrated,
// "draining" once SIGTERM arrives (readiness fails so the load balancer stops routing here,
// while in-flight and straggler requests are still served).
type Readiness struct {
	initialized atomic.Bool
	draining    atomic.Bool
}

func (r *Readiness) SetInitialized()   { r.initialized.Store(true) }
func (r *Readiness) SetDraining()      { r.draining.Store(true) }
func (r *Readiness) Initialized() bool { return r.initialized.Load() }
func (r *Readiness) Draining() bool    { return r.draining.Load() }

type Config struct {
	Store          *booking.Store
	Auth           *auth.Auth
	Metrics        *obs.Metrics
	Logs           *obs.RingBuffer
	PublicLogs     bool
	OpsPool        *pgxpool.Pool // tiny pool for readiness checks: never starved by request traffic
	Ready          *Readiness
	Logger         *slog.Logger
	Version        string
	RequestTimeout time.Duration
	StartedAt      time.Time
}

type server struct{ Config }

func New(c Config) http.Handler {
	s := &server{c}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(c.Metrics.Registry,
		promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}))
	mux.HandleFunc("GET /logs", s.logs)
	mux.HandleFunc("POST /auth/token", s.issueToken)

	mux.HandleFunc("POST /shows", s.serving(s.authenticated(s.adminOnly(s.createShow))))
	mux.HandleFunc("GET /shows/{id}", s.serving(s.getShow))
	mux.HandleFunc("GET /shows/{id}/audit", s.serving(s.auditShow))
	mux.HandleFunc("POST /shows/{id}/reserve", s.serving(s.authenticated(s.reserve)))
	mux.HandleFunc("GET /reservations/{id}", s.serving(s.authenticated(s.getReservation)))
	mux.HandleFunc("POST /reservations/{id}/cancel", s.serving(s.authenticated(s.cancel)))

	return s.observe(s.recoverPanics(mux))
}

// quietRoutes are polled by probes, scrapers and log followers: logged at debug level when
// successful, so they never drown out (or, for /logs, feed back into) the request log.
var quietRoutes = map[string]bool{
	"GET /healthz": true, "GET /readyz": true, "GET /metrics": true, "GET /logs": true,
}

// observe assigns the correlation id, then records metrics and exactly one access-log line.
// It must wrap the mux directly: the mux sets r.Pattern on the request it receives, and we
// read it afterwards to label metrics by route rather than by raw path.
func (s *server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if !requestIDRE.MatchString(rid) {
			rid = newRequestID()
		}
		w.Header().Set("X-Request-ID", rid)
		r = r.WithContext(obs.WithRequest(r.Context(), s.Logger, rid))
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}

		s.Metrics.InFlight.Inc()
		defer s.Metrics.InFlight.Dec()
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		s.Metrics.ObserveHTTP(r.Method, route, rec.status, elapsed)

		level := slog.LevelInfo
		switch {
		case rec.status >= 500:
			level = slog.LevelError
		case quietRoutes[route] && rec.status < 400:
			level = slog.LevelDebug
		}
		attrs := append([]slog.Attr{
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("route", route),
			slog.Int("status", rec.status),
			slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
		}, obs.Annotations(r.Context())...)
		obs.Logger(r.Context()).LogAttrs(context.Background(), level, "http_request", attrs...)
	})
}

func (s *server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.Metrics.Panics.Inc()
				obs.Logger(r.Context()).Error("panic", slog.Any("panic", v), slog.String("stack", string(debug.Stack())))
				writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// serving rejects business requests (503, retryable) until the database is initialized, and
// bounds each request's lifetime.
func (s *server) serving(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.Ready.Initialized() {
			w.Header().Set("Retry-After", "2")
			writeError(w, r, http.StatusServiceUnavailable, "not_ready", "service is starting; retry shortly")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.RequestTimeout)
		defer cancel()
		next(w, r.WithContext(ctx))
	}
}

func (s *server) authenticated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Auth.FromRequest(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="seat-reservation"`)
			writeError(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}
		obs.Annotate(r.Context(), slog.String("user_id", p.UserID))
		next(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	}
}

func (s *server) adminOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if p, _ := auth.PrincipalFrom(r.Context()); p.Role != auth.RoleAdmin {
			writeError(w, r, http.StatusForbidden, "forbidden", "admin token required")
			return
		}
		next(w, r)
	}
}

var requestIDRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// recorder captures the status code. Unwrap lets http.ResponseController reach the real
// writer (used by the log follower to flush and extend its write deadline).
type recorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *recorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
