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
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if maxConns < 2 || maxConns > 500 {
		errs = append(errs, errors.New("DB_MAX_CONNS must be between 2 and 500"))
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
