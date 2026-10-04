package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omkar619-dev/seat-reservation/internal/auth"
	"github.com/omkar619-dev/seat-reservation/internal/booking"
	"github.com/omkar619-dev/seat-reservation/internal/obs"
)

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "seat-reservation",
		"version": s.Version,
		"endpoints": []string{
			"POST /auth/token                 {user_id, role?}  (admin role needs X-Admin-Key)",
			"POST /shows                      admin: {name, seats[], price_paise, per_user_limit?}",
			"GET  /shows/{id}                 seat states + counts (?seats=false for counts only)",
			"GET  /shows/{id}/audit           cross-table reconciliation report",
			"POST /shows/{id}/reserve         user: {seats[], idempotency_key} or Idempotency-Key header",
			"GET  /reservations/{id}          owner",
			"POST /reservations/{id}/cancel   owner",
			"GET  /healthz /readyz /metrics /logs?limit=200&q=&follow=1",
		},
	})
}

// healthz is liveness: the process is up and serving HTTP. It deliberately does not touch
// the database, so a database outage never causes the platform to restart healthy pods.
func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"version":  s.Version,
		"uptime_s": int(time.Since(s.StartedAt).Seconds()),
	})
}

// readyz is readiness: 200 only if migrations ran, we are not draining, and the database
// answers right now. Otherwise 503 (fail closed).
func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	switch {
	case s.Ready.Draining():
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	case !s.Ready.Initialized():
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "starting", "database": "initializing"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := s.OpsPool.Ping(ctx); err != nil {
		obs.Logger(r.Context()).Warn("readiness: database unreachable", slog.String("error", err.Error()))
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "database": "unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready", "database": "ok",
		"database_ping_ms": float64(time.Since(start).Microseconds()) / 1000,
	})
}

// logs serves the in-memory tail of this instance's structured logs as NDJSON.
// ?follow=1 streams new lines (like `tail -f`) for up to 15 minutes.
func (s *server) logs(w http.ResponseWriter, r *http.Request) {
	if !s.PublicLogs {
		writeError(w, r, http.StatusNotFound, "not_found", "log access is disabled")
		return
	}
	q := r.URL.Query()
	limit := 200
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = min(n, 5000)
	}
	filter := q.Get("q")
	follow := q.Get("follow") == "1" || q.Get("follow") == "true"

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	lines, cursor := s.Logs.Since(0, filter, limit)
	writeLines(w, lines)
	if !follow {
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{}) // a follower outlives the server's WriteTimeout
	_ = rc.Flush()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	stop := time.After(15 * time.Minute)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			lines, cursor = s.Logs.Since(cursor, filter, 0)
			if len(lines) > 0 {
				writeLines(w, lines)
				if err := rc.Flush(); err != nil {
					return
				}
			}
		}
	}
}

func writeLines(w http.ResponseWriter, lines [][]byte) {
	for _, l := range lines {
		_, _ = w.Write(l)
		_, _ = w.Write([]byte{'\n'})
	}
}

func (s *server) issueToken(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if !decodeJSON(w, r, &body, 4<<10) {
		return
	}
	token, exp, err := s.Auth.Issue(body.UserID, body.Role, r.Header.Get("X-Admin-Key"))
	switch {
	case errors.Is(err, auth.ErrBadAdminKey):
		writeError(w, r, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, auth.ErrInvalidUserID), errors.Is(err, auth.ErrUnknownRole):
		writeError(w, r, http.StatusBadRequest, "invalid_request", err.Error())
	case err != nil:
		obs.Logger(r.Context()).Error("token signing failed", slog.String("error", err.Error()))
		writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	default:
		role := body.Role
		if role == "" {
			role = auth.RoleUser
		}
		obs.Annotate(r.Context(), slog.String("user_id", body.UserID), slog.String("role", role))
		writeJSON(w, http.StatusOK, map[string]any{
			"token": token, "token_type": "Bearer", "user_id": body.UserID, "role": role,
			"expires_at": exp.UTC(),
		})
	}
}

func (s *server) createShow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name         string   `json:"name"`
		Seats        []string `json:"seats"`
		PricePaise   *int64   `json:"price_paise"`
		PerUserLimit *int     `json:"per_user_limit"`
	}
	if !decodeJSON(w, r, &body, 4<<20) {
		return
	}
	if body.PricePaise == nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request", "price_paise is required (integer paise)")
		return
	}
	limit := 0
	if body.PerUserLimit != nil {
		if *body.PerUserLimit < 1 {
			writeError(w, r, http.StatusBadRequest, "invalid_request", "per_user_limit must be at least 1")
			return
		}
		limit = *body.PerUserLimit
	}
	st, err := s.Store.CreateShow(r.Context(), booking.CreateShowInput{
		Name: body.Name, Seats: body.Seats, PricePaise: *body.PricePaise, PerUserLimit: limit,
	})
	if err != nil {
		storeError(w, r, err)
		return
	}
	obs.Annotate(r.Context(), slog.String("show_id", st.ID), slog.Int("total_seats", st.TotalSeats))
	w.Header().Set("Location", "/shows/"+st.ID)
	writeJSON(w, http.StatusCreated, st)
}

func (s *server) getShow(w http.ResponseWriter, r *http.Request) {
	withSeats := r.URL.Query().Get("seats") != "false"
	st, err := s.Store.ShowState(r.Context(), r.PathValue("id"), withSeats)
	if err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) auditShow(w http.ResponseWriter, r *http.Request) {
	rep, err := s.Store.Audit(r.Context(), r.PathValue("id"))
	if err != nil {
		storeError(w, r, err)
		return
	}
	if !rep.OK {
		s.Metrics.ReconcileFailures.Inc()
		obs.Logger(r.Context()).Error("RECONCILIATION FAILED", slog.String("show_id", rep.ShowID), slog.Any("checks", rep.Checks))
	}
	writeJSON(w, http.StatusOK, rep)
}

// reserve: identity comes only from the token. A user_id in the body is never used; it is
// detected and counted (auth_spoof_attempts_total) so the attempt is observable.
func (s *server) reserve(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, _ := auth.PrincipalFrom(ctx)
	showID := r.PathValue("id")
	var body struct {
		Seats          []string        `json:"seats"`
		IdempotencyKey string          `json:"idempotency_key"`
		UserID         json.RawMessage `json:"user_id"`
	}
	if !decodeJSON(w, r, &body, 64<<10) {
		return
	}
	obs.Annotate(ctx, slog.String("show_id", showID), slog.Any("seats", body.Seats))

	if len(body.UserID) > 0 && string(body.UserID) != "null" {
		var claimed string
		_ = json.Unmarshal(body.UserID, &claimed)
		if claimed != p.UserID {
			s.Metrics.SpoofAttempts.Inc()
			obs.Annotate(ctx, slog.String("ignored_body_user_id", claimed))
		}
	}

	key, problem := idempotencyKey(r.Header.Get("Idempotency-Key"), body.IdempotencyKey)
	if problem != "" {
		writeError(w, r, http.StatusBadRequest, "invalid_request", problem)
		return
	}

	res, err := s.Store.Reserve(ctx, booking.ReserveRequest{
		ShowID: showID, UserID: p.UserID, Seats: body.Seats, IdempotencyKey: key,
	})
	var decline *booking.DeclineError
	switch {
	case errors.As(err, &decline):
		s.Metrics.ReservationsDeclined.WithLabelValues(string(decline.Reason), string(decline.Stage)).Inc()
		obs.Annotate(ctx, slog.String("outcome", "declined"),
			slog.String("reason", string(decline.Reason)), slog.String("stage", string(decline.Stage)))
		writeDecline(w, r, decline)
		return
	case err != nil:
		storeError(w, r, err)
		return
	}

	rv := res.Reservation
	obs.Annotate(ctx, slog.String("reservation_id", rv.ID))
	w.Header().Set("Location", "/reservations/"+rv.ID)
	if res.Replayed {
		// The original response is replayed (same status, same reservation); counted with the
		// declines because it moved nothing.
		s.Metrics.ReservationsDeclined.WithLabelValues(string(booking.ReasonIdempotentReplay), string(res.Stage)).Inc()
		obs.Annotate(ctx, slog.String("outcome", "replayed"),
			slog.String("reason", string(booking.ReasonIdempotentReplay)), slog.String("stage", string(res.Stage)))
		w.Header().Set("Idempotent-Replayed", "true")
	} else {
		s.Metrics.ReservationsConfirmed.Inc()
		s.Metrics.SeatsConfirmed.Add(float64(len(rv.Seats)))
		obs.Annotate(ctx, slog.String("outcome", "confirmed"), slog.Int64("amount_paise", rv.AmountPaise))
		w.Header().Set("Idempotent-Replayed", "false")
	}
	writeJSON(w, http.StatusCreated, rv)
}

// idempotencyKey accepts the key from the Idempotency-Key header or the body field; if both
// are present they must agree.
func idempotencyKey(header, body string) (string, string) {
	header, body = strings.TrimSpace(header), strings.TrimSpace(body)
	switch {
	case header != "" && body != "" && header != body:
		return "", "Idempotency-Key header and idempotency_key body field differ"
	case header != "":
		return header, ""
	case body != "":
		return body, ""
	default:
		return "", "an idempotency key is required (Idempotency-Key header or idempotency_key field)"
	}
}

func (s *server) getReservation(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	rv, err := s.Store.Reservation(r.Context(), r.PathValue("id"))
	if err == nil && rv.UserID != p.UserID && p.Role != auth.RoleAdmin {
		err = booking.ErrNotOwner
	}
	if err != nil {
		storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rv)
}

func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, _ := auth.PrincipalFrom(ctx)
	id := r.PathValue("id")
	obs.Annotate(ctx, slog.String("reservation_id", id))
	rv, changed, err := s.Store.Cancel(ctx, id, p.UserID)
	if err != nil {
		storeError(w, r, err)
		return
	}
	obs.Annotate(ctx, slog.String("show_id", rv.ShowID))
	if changed {
		s.Metrics.ReservationsCancelled.Inc()
		s.Metrics.SeatsReleased.Add(float64(len(rv.Seats)))
		obs.Annotate(ctx, slog.String("outcome", "cancelled"))
	} else {
		obs.Annotate(ctx, slog.String("outcome", "already_cancelled"))
	}
	writeJSON(w, http.StatusOK, rv)
}
