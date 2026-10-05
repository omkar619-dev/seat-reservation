package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeJSON(t *testing.T) {
	type body struct {
		Price int64 `json:"price_paise"`
	}
	cases := []struct {
		name     string
		in       string
		max      int64
		wantOK   bool
		wantCode int
		wantErr  string
	}{
		{"valid", `{"price_paise": 25000}`, 1024, true, 0, ""},
		{"float is not paise", `{"price_paise": 250.5}`, 1024, false, 400, "invalid_request"},
		{"two objects", `{"price_paise": 1}{"price_paise": 2}`, 1024, false, 400, "invalid_request"},
		{"object larger than the limit", `{"price_paise": 1, "pad": "` + strings.Repeat("x", 4096) + `"}`, 64, false, 413, "body_too_large"},
		{"trailing data past the limit", `{"price_paise": 1}` + strings.Repeat(" ", 4096) + "x", 64, false, 413, "body_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.in))
			var b body
			ok := decodeJSON(w, r, &b, tc.max)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (body %s)", ok, tc.wantOK, w.Body.String())
			}
			if tc.wantOK {
				return
			}
			if w.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantCode)
			}
			var resp map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp["error"] != tc.wantErr {
				t.Fatalf("error code = %v, want %q (body %s)", resp["error"], tc.wantErr, w.Body.String())
			}
		})
	}
}

func TestMethodLabelIsBounded(t *testing.T) {
	for _, m := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		if got := methodLabel(m); got != m {
			t.Errorf("methodLabel(%q) = %q, want it unchanged", m, got)
		}
	}
	// Any other token (Go's server accepts arbitrary method names) collapses to one series.
	for _, m := range []string{"FOO", "get", "PROPFIND", strings.Repeat("X", 200)} {
		if got := methodLabel(m); got != "OTHER" {
			t.Errorf("methodLabel(%q) = %q, want OTHER", m, got)
		}
	}
}

func TestPrincipalFailsClosed(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/shows/x/reserve", nil) // no authenticated() wrapper
	if _, ok := principal(w, r); ok {
		t.Fatal("principal() accepted a request without a verified token")
	}
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestSeatsForLogIsBounded(t *testing.T) {
	many := make([]string, 500)
	for i := range many {
		many[i] = strings.Repeat("Z", 1000)
	}
	got := seatsForLog(many)
	if len(got) != 11 || got[10] != "...+490 more" {
		t.Fatalf("got %d entries, last %q; want 10 labels + a count", len(got), got[len(got)-1])
	}
	if len(got[0]) > 35 {
		t.Fatalf("label not truncated: %d bytes", len(got[0]))
	}
	if small := seatsForLog([]string{"A1", "A2"}); len(small) != 2 || small[1] != "A2" {
		t.Fatalf("small list changed: %v", small)
	}
}
