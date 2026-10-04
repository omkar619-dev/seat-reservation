// Package auth issues and verifies bearer tokens (HS256 JWTs).
//
// Identity always comes from a verified token, never from a request body. POST /auth/token
// is a demo identity provider: it mints a user token for any user_id (so a load test can
// act as thousands of users) and an admin token only with the ADMIN_KEY. In production the
// service would only verify tokens issued by a real IdP (OIDC), and this endpoint would go.
package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	RoleUser  = "user"
	RoleAdmin = "admin"
	issuer    = "seat-reservation"
)

var userIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$`)

type Principal struct {
	UserID string
	Role   string
}

type claims struct {
	Role string `json:"role"`
	jwt.RegisteredClaims
}

type Auth struct {
	secret   []byte
	adminKey string
	ttl      time.Duration
}

func New(secret, adminKey string, ttl time.Duration) *Auth {
	return &Auth{secret: []byte(secret), adminKey: adminKey, ttl: ttl}
}

var (
	ErrInvalidUserID = errors.New("user_id must be 1-64 chars: letters, digits, '_', '.', '@' or '-'")
	ErrBadAdminKey   = errors.New("admin role requires a valid X-Admin-Key header")
	ErrUnknownRole   = errors.New(`role must be "user" or "admin"`)
)

// Issue mints a token. Admin tokens require the admin key (constant-time comparison).
func (a *Auth) Issue(userID, role, adminKey string) (string, time.Time, error) {
	if !userIDRE.MatchString(userID) {
		return "", time.Time{}, ErrInvalidUserID
	}
	switch role {
	case "", RoleUser:
		role = RoleUser
	case RoleAdmin:
		if a.adminKey == "" || subtle.ConstantTimeCompare([]byte(adminKey), []byte(a.adminKey)) != 1 {
			return "", time.Time{}, ErrBadAdminKey
		}
	default:
		return "", time.Time{}, ErrUnknownRole
	}
	now := time.Now()
	exp := now.Add(a.ttl)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	})
	signed, err := tok.SignedString(a.secret)
	return signed, exp, err
}

// Verify checks signature, algorithm (HS256 only: no "alg: none" tricks), issuer and expiry.
func (a *Auth) Verify(token string) (Principal, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return a.secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return Principal{}, fmt.Errorf("invalid token: %w", err)
	}
	if !userIDRE.MatchString(c.Subject) || (c.Role != RoleUser && c.Role != RoleAdmin) {
		return Principal{}, errors.New("invalid token claims")
	}
	return Principal{UserID: c.Subject, Role: c.Role}, nil
}

// FromRequest extracts and verifies "Authorization: Bearer <token>".
func (a *Auth) FromRequest(r *http.Request) (Principal, error) {
	h := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return Principal{}, errors.New("missing bearer token")
	}
	return a.Verify(strings.TrimSpace(token))
}

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
