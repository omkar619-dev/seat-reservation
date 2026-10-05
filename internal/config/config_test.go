package config

import (
	"strings"
	"testing"
	"time"
)

// setEnv clears every variable Load reads, then applies overrides, so tests don't depend on the
// shell they run in.
func setEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for _, k := range []string{"PORT", "DATABASE_URL", "JWT_SECRET", "ADMIN_KEY", "APP_ENV",
		"DB_MAX_CONNS", "TOKEN_TTL", "RESERVE_PRECHECK", "PUBLIC_LOGS", "LOG_BUFFER_LINES",
		"LOG_STDOUT_RATE", "REQUEST_TIMEOUT", "SHUTDOWN_DELAY", "AUDIT_INTERVAL", "AUDIT_SHOWS",
		"LOG_LEVEL"} {
		t.Setenv(k, "")
	}
	for k, v := range overrides {
		t.Setenv(k, v)
	}
}

func TestLoad(t *testing.T) {
	const db = "postgres://u:p@localhost:5432/db"
	cases := []struct {
		name    string
		env     map[string]string
		wantErr []string // substrings that must all appear; empty = must succeed
	}{
		{"development defaults", map[string]string{"DATABASE_URL": db}, nil},
		{"database url required", map[string]string{}, []string{"DATABASE_URL is required"}},
		{"zero request timeout would 503 everything", map[string]string{"DATABASE_URL": db, "REQUEST_TIMEOUT": "0s"},
			[]string{"REQUEST_TIMEOUT must be > 0"}},
		{"zero token ttl", map[string]string{"DATABASE_URL": db, "TOKEN_TTL": "0s"}, []string{"TOKEN_TTL must be > 0"}},
		{"audit shows out of range", map[string]string{"DATABASE_URL": db, "AUDIT_SHOWS": "0"},
			[]string{"AUDIT_SHOWS must be between 1 and 50"}},
		{"negative audit interval", map[string]string{"DATABASE_URL": db, "AUDIT_INTERVAL": "-1s"},
			[]string{"AUDIT_INTERVAL must not be negative"}},
		{"unparsable values are all reported at once", map[string]string{"DATABASE_URL": db,
			"DB_MAX_CONNS": "forty", "REQUEST_TIMEOUT": "soon"}, []string{"DB_MAX_CONNS", "REQUEST_TIMEOUT"}},
		{"production needs real secrets", map[string]string{"DATABASE_URL": db, "APP_ENV": "production"},
			[]string{"JWT_SECRET must be set", "ADMIN_KEY must be set"}},
		{"production with secrets", map[string]string{"DATABASE_URL": db, "APP_ENV": "production",
			"JWT_SECRET": strings.Repeat("s", 32), "ADMIN_KEY": strings.Repeat("a", 16)}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			c, err := Load()
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want errors %q, got none", tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			_ = c
		})
	}
}

func TestDevelopmentDefaults(t *testing.T) {
	setEnv(t, map[string]string{"DATABASE_URL": "postgres://localhost/db"})
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RequestTimeout != 30*time.Second || c.DBMaxConns != 40 || !c.Precheck || c.AdminKey != "dev-admin-key" ||
		c.AuditInterval != 30*time.Second || c.AuditShows != 5 || c.LogStdoutRate != 400 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}
