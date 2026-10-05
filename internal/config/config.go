// Package config reads all settings from the environment (12-factor), with safe local
// defaults and strict requirements when APP_ENV=production.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port           string
	DatabaseURL    string
	DBMaxConns     int32
	JWTSecret      string
	AdminKey       string
	TokenTTL       time.Duration
	Precheck       bool
	PublicLogs     bool
	LogBufferLines int
	LogStdoutRate  int
	LogLevel       slog.Level
	RequestTimeout time.Duration
	ShutdownDelay  time.Duration
	AuditInterval  time.Duration
	AuditShows     int
	Production     bool
}

func Load() (Config, error) {
	c := Config{
		Port:        env("PORT", "8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		AdminKey:    os.Getenv("ADMIN_KEY"),
		Production:  strings.EqualFold(os.Getenv("APP_ENV"), "production"),
	}
	var errs []error
	maxConns := envInt("DB_MAX_CONNS", 40, &errs)
	c.DBMaxConns = int32(maxConns)
	c.TokenTTL = envDuration("TOKEN_TTL", 24*time.Hour, &errs)
	c.Precheck = envBool("RESERVE_PRECHECK", true, &errs)
	c.PublicLogs = envBool("PUBLIC_LOGS", true, &errs)
	c.LogBufferLines = envInt("LOG_BUFFER_LINES", 20000, &errs)
	c.LogStdoutRate = envInt("LOG_STDOUT_RATE", 400, &errs) // lines/s; Railway drops above 500/s. 0 = unlimited
	c.RequestTimeout = envDuration("REQUEST_TIMEOUT", 30*time.Second, &errs)
	c.ShutdownDelay = envDuration("SHUTDOWN_DELAY", 3*time.Second, &errs)
	c.AuditInterval = envDuration("AUDIT_INTERVAL", 30*time.Second, &errs) // 0 disables the scheduled auditor
	c.AuditShows = envInt("AUDIT_SHOWS", 5, &errs)
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if maxConns < 2 || maxConns > 500 {
		errs = append(errs, errors.New("DB_MAX_CONNS must be between 2 and 500"))
	}
	// Parsing alone accepts nonsense like REQUEST_TIMEOUT=0, which would 503 every request.
	if c.RequestTimeout <= 0 {
		errs = append(errs, errors.New("REQUEST_TIMEOUT must be > 0"))
	}
	if c.TokenTTL <= 0 {
		errs = append(errs, errors.New("TOKEN_TTL must be > 0"))
	}
	if c.ShutdownDelay < 0 {
		errs = append(errs, errors.New("SHUTDOWN_DELAY must not be negative"))
	}
	if c.AuditInterval < 0 {
		errs = append(errs, errors.New("AUDIT_INTERVAL must not be negative (0 disables the auditor)"))
	}
	if c.AuditShows < 1 || c.AuditShows > 50 {
		errs = append(errs, errors.New("AUDIT_SHOWS must be between 1 and 50"))
	}
	if c.LogBufferLines < 1 || c.LogBufferLines > 1_000_000 {
		errs = append(errs, errors.New("LOG_BUFFER_LINES must be between 1 and 1000000"))
	}
	if c.LogStdoutRate < 0 {
		errs = append(errs, errors.New("LOG_STDOUT_RATE must not be negative (0 = unlimited)"))
	}
	if c.Production {
		if len(c.JWTSecret) < 32 {
			errs = append(errs, errors.New("JWT_SECRET must be set (32+ chars) when APP_ENV=production"))
		}
		if len(c.AdminKey) < 16 {
			errs = append(errs, errors.New("ADMIN_KEY must be set (16+ chars) when APP_ENV=production"))
		}
	} else {
		if c.JWTSecret == "" {
			c.JWTSecret = "dev-only-insecure-jwt-secret-change-me"
		}
		if c.AdminKey == "" {
			c.AdminKey = "dev-admin-key"
		}
	}
	return c, errors.Join(errs...)
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int, errs *[]error) int {
	v := env(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func envBool(key string, def bool, errs *[]error) bool {
	v := env(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	v := env(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
