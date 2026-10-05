package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/omkar619-dev/seat-reservation/internal/booking"
	"github.com/omkar619-dev/seat-reservation/internal/db"
	"github.com/omkar619-dev/seat-reservation/internal/obs"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": code, "message": msg, "request_id": obs.RequestID(r.Context()),
	})
}

// decodeJSON reads exactly one JSON object of at most maxBytes. Integer fields decode into
// int64/int, so a float like 250.50 for price_paise is rejected rather than truncated.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		bodyError(w, r, err, "invalid JSON body: "+err.Error())
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		bodyError(w, r, err, "body must be a single JSON object")
		return false
	}
	return true
}

// bodyError answers 413 when the body ran past the size limit, otherwise 400 with msg.
func bodyError(w http.ResponseWriter, r *http.Request, err error, msg string) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, r, http.StatusRequestEntityTooLarge, "body_too_large",
			fmt.Sprintf("request body exceeds %d bytes", tooBig.Limit))
		return
	}
	writeError(w, r, http.StatusBadRequest, "invalid_request", msg)
}

// writeDecline: a domain "no" is always 409 with a machine-readable reason, never a 5xx.
func writeDecline(w http.ResponseWriter, r *http.Request, d *booking.DeclineError) {
	body := map[string]any{
		"error":      string(d.Reason),
		"message":    d.Error(),
		"request_id": obs.RequestID(r.Context()),
	}
	switch d.Reason {
	case booking.ReasonSeatTaken:
		body["unavailable_seats"] = d.Unavailable
	case booking.ReasonPerUserLimit:
		body["limit"] = d.Limit
		body["requested"] = d.Requested
		if d.Held >= 0 {
			body["held"] = d.Held
		}
	}
	writeJSON(w, http.StatusConflict, body)
}

// storeError maps non-decline errors. Client mistakes are 4xx; a 5xx means the database is
// unavailable/overloaded (503, retryable) or a bug (500).
func storeError(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	var ve *booking.ValidationError
	switch {
	case errors.As(err, &ve):
		body := map[string]any{"error": "invalid_request", "message": ve.Msg, "request_id": obs.RequestID(ctx)}
		if len(ve.UnknownSeats) > 0 {
			body["unknown_seats"] = ve.UnknownSeats
		}
		writeJSON(w, http.StatusBadRequest, body)
	case errors.Is(err, booking.ErrShowNotFound):
		writeError(w, r, http.StatusNotFound, "show_not_found", "show not found")
	case errors.Is(err, booking.ErrReservationNotFound):
		writeError(w, r, http.StatusNotFound, "reservation_not_found", "reservation not found")
	case errors.Is(err, booking.ErrNotOwner):
		obs.Annotate(ctx, slog.String("outcome", "forbidden"))
		writeError(w, r, http.StatusForbidden, "forbidden", "only the reservation's owner can do this")
	case errors.Is(ctx.Err(), context.Canceled):
		// The client went away; nobody will read a response. 499 keeps our 5xx metrics honest.
		obs.Annotate(ctx, slog.String("outcome", "client_closed_request"))
		w.WriteHeader(499)
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || unavailable(err):
		obs.Logger(ctx).Warn("database unavailable or overloaded", slog.String("error", err.Error()))
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusServiceUnavailable, "unavailable", "temporarily unavailable; retry")
	default:
		obs.Logger(ctx).Error("unexpected error", slog.String("error", err.Error()))
		writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// unavailable reports errors that mean "the dependency is down or overloaded", not "bug".
func unavailable(err error) bool {
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) || pgconn.Timeout(err) {
		return true
	}
	code := db.PgCode(err)
	switch {
	case strings.HasPrefix(code, "08"), // connection exception
		strings.HasPrefix(code, "53"),                     // insufficient resources (too many connections, ...)
		code == "57014",                                   // statement timeout
		code == "55P03",                                   // lock timeout (after retries)
		code == "57P01", code == "57P02", code == "57P03": // admin shutdown, crash, cannot connect now
		return true
	}
	return false
}
