# seat-reservation

A seat reservation API built for on-sale bursts: thousands of buyers hit the same seats at the
same moment, no seat may be sold twice, no user may go over the per-show limit, and a retried
request must never book twice. Go 1.26, Postgres 17, pgx.

- **Live:** https://16-4-27-248.sslip.io ([`/readyz`](https://16-4-27-248.sslip.io/readyz), [`/metrics`](https://16-4-27-248.sslip.io/metrics),
  [`/logs`](https://16-4-27-248.sslip.io/logs?limit=50)). One AWS t3.micro in Mumbai running Postgres, the app and
  Caddy (HTTPS) with Docker Compose; see [Deploy](#deploy).
- **Admin key** (creates shows, so the burst needs it too): shared in the submission email.
- **Design write-up:** [WRITEUP.md](WRITEUP.md) covers the atomic decision, idempotency, holds,
  partitions, observability (including what the live box showed under load), AI usage and the next
  steps.

## Run locally

Needs Docker with Compose v2 (Go 1.26+ only for `make test`, `make run` and `make build`; the
burst falls back to Docker). From a clean checkout:

```bash
docker compose up --build
```

This builds the same Dockerfile and runtime image the live deployment runs (static binary on
distroless, non-root) and starts it next to Postgres 17.

| | |
|---|---|
| API | `http://localhost:8080` (`APP_PORT` to change) |
| Postgres | `localhost:55432`, user/password/database `seats` (`PG_PORT` to change) |
| Admin key | `dev-admin-key` (compose runs with `APP_ENV=development`) |

The HTTP listener comes up before the database is migrated, and business routes return 503
until it is. Wait for readiness:

```bash
curl -s localhost:8080/readyz
# {"database":"ok","database_ping_ms":5.16,"status":"ready"}
```

To run with the read-only precheck off, so every request goes through the transaction:
`RESERVE_PRECHECK=false docker compose up --build`.

| Make target | What it does |
|---|---|
| `make up` / `make down` | stack in the background / stop it and delete the Postgres volume |
| `make test` | start Postgres, run all tests with `-race`, including the real-Postgres concurrency tests |
| `make test-short` | tests without a database (the DB tests skip) |
| `make run` | Postgres in Docker, server via `go run` |
| `make build` | `bin/server` and `bin/burst` |
| `make burst BASE_URL=...` | the burst below (default `http://localhost:8080`) |
| `make logs` | follow the app container's stdout |

## Burst test (one command)

```bash
./burst.sh http://localhost:8080
ADMIN_KEY=<admin key> ./burst.sh https://16-4-27-248.sslip.io
make burst BASE_URL=https://16-4-27-248.sslip.io             # same; ADMIN_KEY comes from the environment
./burst.sh https://16-4-27-248.sslip.io -concurrency 3000    # flags go after the URL
```

`burst.sh` runs `go run ./cmd/burst` when Go is installed. Without Go it builds the Dockerfile's
`burst` target and runs it in Docker, rewriting `localhost`/`127.0.0.1` to
`host.docker.internal`. It needs the server's admin key because it creates a fresh show; locally
the default `dev-admin-key` works. Exit code: `0` no check failed (WARN allowed), `1` a check
failed, `2` aborted before the checks (service never ready, admin key rejected, bad flags).

What it does:

1. Waits up to 3 minutes for `/readyz` (survives a cold start). Creates a fresh show of 1,000
   seats (20 rows x 50, limit 4, 25000 paise each) and mints 5,000 user tokens.
2. **Stampede:** 20,000 reserve requests released through one gate at 1,000 concurrency, one
   HTTP/1.1 connection each, warmed up first. Half of them target 5 hot seats in the middle of
   row A. The rest pick front-biased seats, 20% of those ask for 2 adjacent seats. 10% of all
   requests re-send an earlier request with the same idempotency key, and 5% carry a spoofed
   `user_id` in the body. Transport errors and 5xx are retried with the same key, as a real
   client should. `GET /shows/{id}` is polled every 250 ms throughout.
3. **Probes**, mostly on the last row, which the stampede never touches.
4. **Reconciliation:** the client's own ledger (built only from what the API returned) against
   `GET /shows/{id}` seat by seat, against `/metrics`, and against `GET /shows/{id}/audit`.

Flags and defaults: `-requests 20000 -concurrency 1000 -users 5000 -rows 20 -cols 50` (at least
22 columns: the last row holds the probe seats) `-hot-seats 5 -hot-share 0.5 -retry-share 0.1 -multi-share 0.2 -spoof-share 0.05 -limit 4
-price-paise 25000 -timeout 60s -ready-wait 3m -seed <now> -out <summary.json>
-admin-key <$ADMIN_KEY or dev-admin-key>`. The header line prints the seed, so a failing plan can
be replayed with `-seed <n>`.

Checks, as printed:

| Check | Passes when |
|---|---|
| `no seat sold twice` | no seat appears in two different reservations across all 201 responses |
| `zero 5xx` | no attempt got a 5xx (the client retries them, but any 5xx fails this check) |
| `zero transport errors (after same-key retries)` | every request got an HTTP answer within 4 attempts |
| `hot seats: exactly one winner each, everyone else 409` | each hot seat has one winning reservation; every other attempt is a 409 |
| `idempotent retries move nothing extra` | every key maps to at most one reservation and at most one fresh 201 |
| `identity comes from the token` | every reservation belongs to the token's user, spoofed body or not |
| `per-user limit during the stampede` | no user holds more than the limit |
| `invariant held during the burst` | every poll: available + held + confirmed == total, and confirmed never decreases |
| `per-user limit under 10 parallel requests` | one user, 10 parallel single-seat requests, limit 4: exactly 4 x 201 and 6 x 409 `per_user_limit` |
| `same key + different seats -> 409` | 10 winners re-send their key for another seat: all 409 `idempotency_key_reused` |
| `exact retry returns the original reservation` | same key and seats: 201 replay with the same `reservation_id` |
| `spoofed body user_id is ignored` | a reservation with a spoofed body belongs to the token's user |
| `only the owner can cancel` | non-owner cancel is refused (403) and the seat stays confirmed |
| `owner cancel makes the seat re-bookable` | owner cancel returns 200, then another user books the seat |
| `a hot seat stays sold after the storm` | a late request for a hot seat gets 409 `seat_taken` |
| `final invariant` | available + held + confirmed == total_seats |
| `API state == what clients were told` | the confirmed seats in `GET /shows/{id}` are exactly the client ledger, seat by seat |
| `metrics gauges == API state` | `seats_available/confirmed/total/reconciled` agree with `GET /shows/{id}` |
| `metrics counters == observed outcomes` | counter deltas equal what the client saw (WARN, not FAIL: exact only with one replica and no other traffic) |
| `server-side audit (cross-table reconciliation)` | all 8 audit checks pass |

Results against the live deployment (client: a laptop in India on home broadband; server: the
t3.micro), every check passing:

| Concurrency | Requests | Throughput | p50 | p95 | p99 | max | 5xx |
|---|---|---|---|---|---|---|---|
| 1,000 | 20,000 | 1,043 req/s | 0.90 s | 1.34 s | 1.99 s | 3.47 s | 0 |
| 3,000 | 20,000 | 572 req/s | 4.79 s | 7.88 s | 9.57 s | 13.8 s | 0 |

At 3,000 connections the box is CPU-bound on TLS termination in Caddy, not on the app or Postgres
([WRITEUP.md section 5](WRITEUP.md#what-the-live-box-showed)). Run to run, three warm 1,000-connection
runs gave 978-1,043 req/s and p99 2.0-3.2 s; the first run after a reboot was about half as fast
(cold caches). Rare single requests took 20-30 s end to end, but the app's own
`http_request_duration_seconds` shows every reserve request (60,102 across those runs) finished
within 5 s inside the app, so that time was spent in the proxy or on the network path (the shape
matches TCP retransmit backoff). Locally (Docker on a MacBook, client on the same machine) the same
burst runs at 19-22k req/s with p99 under 200 ms.

Sample output, trimmed, from a warm live 1,000-connection run of the deployed version (`1786d0e`):

```text
== seat-reservation burst c2127b1b -> https://16-4-27-248.sslip.io (seed 1791183465308652000)
ready after 54ms
show 01a10ada-abac-7813-bb23-20e700ebbabf: 1000 seats (20 rows x 50), limit 4, hot seats [A26 A25 A27 A24 A28]
minted 5000 user tokens
stampede: 20000 requests, 1000 concurrent, 5000 users ...
stampede done in 19.637s (1018 req/s); latency p50 879ms p95 1.698s p99 2.365s max 6.745s

outcomes (stampede + probes)
  201 confirmed (new)              730
  201 idempotent replay            126
  409 idempotency_key_reused         10
  409 per_user_limit                  6
  409 seat_taken                  19162
  5xx                                0
  no response (transport)            0

checks
  [PASS] no seat sold twice
         0 seats appeared in two different reservations across 839 confirmed responses
  [PASS] zero 5xx
         0 5xx responses across the stampede
  [PASS] zero transport errors (after same-key retries)
         0 requests never got a response
  [PASS] hot seats: exactly one winner each, everyone else 409
         A26: 1987 attempts, 1 winner, 0 non-409; A25: 1986 attempts, 1 winner, 0 non-409; A27: 1998 attempts, 1 winner, 0 non-409; A24: 2015 attempts, 1 winner, 0 non-409; A28: 2000 attempts, 1 winner, 0 non-409
  [PASS] idempotent retries move nothing extra
         1883 keys sent more than once (concurrently or later); 0 keys produced more than one reservation; 116 replays served
  ... (11 more [PASS] checks trimmed) ...
  [PASS] API state == what clients were told
         813 seats confirmed by the API, 813 in the client ledger, 0 seat-level mismatches
  [PASS] metrics gauges == API state
         seats_available 187, seats_confirmed 813, seats_total 1000, seats_reconciled 1
  [PASS] metrics counters == observed outcomes
         confirmed +730, replays +126, seat_taken +19162, per_user_limit +6, key_reused +10, cancelled +1
  [PASS] server-side audit (cross-table reconciliation)
         8/8 checks ok 

show 01a10ada-abac-7813-bb23-20e700ebbabf -- inspect: https://16-4-27-248.sslip.io/shows/01a10ada-abac-7813-bb23-20e700ebbabf  https://16-4-27-248.sslip.io/shows/01a10ada-abac-7813-bb23-20e700ebbabf/audit  https://16-4-27-248.sslip.io/logs?q=c2127b1b
RESULT: PASS
```

## Metrics and logs

```bash
BASE=https://16-4-27-248.sslip.io   # or http://localhost:8080
curl -s $BASE/metrics | grep -E '^(reservations_|reservation_seats_|seats_|db_tx_retries_total|db_pool_empty)'
curl -s "$BASE/logs?limit=50"               # last 50 lines as NDJSON (default 200, max 5000)
curl -s "$BASE/logs?q=seat_taken&limit=20"  # substring filter
curl -sN "$BASE/logs?follow=1&q=/reserve"   # live tail, like tail -f (stops after 15 minutes)
curl -s $BASE/shows/<show_id>/audit          # 8 cross-table invariants from one snapshot
```

- **Metrics:** `GET /metrics`, Prometheus text format, no auth. No Prometheus server is bundled:
  point one at it or read it with curl. Seat gauges
  (`seats_available/held/confirmed/total/reconciled{show_id,show_name}`) are read from Postgres
  on every scrape for the 50 most recent shows; counters are per process. `db_up` pings Postgres
  on every scrape, and `show_audit_ok{show_id}` is the result of the scheduled audit (every 30 s,
  5 newest shows): the two signals worth paging on (`show_audit_errors_total` counts audits that
  couldn't run, which also stops the last-run timestamp from advancing). The full list and the
  alerting rules are in [WRITEUP.md section 5](WRITEUP.md#5-observability).
- **Logs:** one JSON line per request. Send `X-Request-ID` (8-128 characters of
  `[A-Za-z0-9._:-]`) to correlate; it is echoed back and logged, otherwise one is generated. The
  burst tags its requests `burst-<run id>-<n>`, so `/logs?q=<run id>` finds them.
- **`GET /logs`** serves an in-memory tail of the last 20,000 lines of the instance that answers.
  It is public by design for this assignment; `PUBLIC_LOGS=false` turns it off (404). Successful
  `/healthz`, `/readyz`, `/metrics` and `/logs` requests log at DEBUG, so they stay out of the
  default view.
- **Container logs** (`docker logs` on the VM, rotated at 5 x 20 MB; `make logs` locally) get
  stdout, which is rate limited to 400 info lines/s, because hosted platforms cap log throughput
  (Railway drops anything above 500 lines/s per replica) and a burst emits about 20k lines/s.
  WARN and ERROR always pass, and drops are reported in-band and counted in
  `log_stdout_lines_dropped_total`. During a burst, use `/logs` and `/metrics`.

A real reserve line:

```json
{"time":"2026-10-04T18:00:59.532336838Z","level":"INFO","msg":"http_request","request_id":"23a8f374b213401e4739c75a","method":"POST","path":"/shows/01a10813-8508-77eb-b8b5-84052b0ef61b/reserve","route":"POST /shows/{id}/reserve","status":409,"duration_ms":1.249,"user_id":"alice","show_id":"01a10813-8508-77eb-b8b5-84052b0ef61b","seats":["A3","A4","A5"],"outcome":"declined","reason":"per_user_limit","stage":"precheck"}
```

## API

**Auth.** Bearer JWTs (HS256, algorithm pinned, issuer and expiry required, 24h TTL).
`POST /auth/token` is a demo identity provider: it mints a user token for any `user_id`, which is
what lets the burst act as 5,000 users. An admin token also needs the `X-Admin-Key` header.
Identity always comes from the token: a `user_id` in a reserve body is ignored and counted in
`auth_spoof_attempts_total`. In production the service would verify tokens from a real IdP and
this endpoint would go away.

**Money** is integer paise (`int64`) end to end. A JSON float such as `250.50` is rejected with 400.

| Method and path | Who | Body / notes |
|---|---|---|
| `POST /auth/token` | anyone | `{"user_id", "role"?}`; `"role":"admin"` needs `X-Admin-Key` |
| `POST /shows` | admin | `{"name", "seats":[labels], "price_paise", "per_user_limit"?}` (limit defaults to 4, max 100; up to 100,000 seats; labels are 1-16 letters, digits, `-` or `_`, starting with a letter or digit) |
| `GET /shows/{id}` | anyone | counts plus every seat's status; `?seats=false` for counts only |
| `GET /shows/{id}/audit` | anyone | reconciliation report (8 checks, one snapshot) |
| `POST /shows/{id}/reserve` | user | `{"seats":[labels], "idempotency_key"?}` or the `Idempotency-Key` header (one is required; if both are sent they must match) |
| `GET /reservations/{id}` | owner or admin | |
| `POST /reservations/{id}/cancel` | owner | releases the seats; cancelling again is a 200 no-op |
| `GET /healthz`, `GET /readyz` | anyone | liveness (never touches the DB) / readiness (DB ping, 503 when not ready) |
| `GET /metrics`, `GET /logs` | anyone | see above |

### Walkthrough

Uses `curl` and `jq`. Responses below are real, from the local stack.

```bash
BASE=http://localhost:8080        # or https://16-4-27-248.sslip.io
ADMIN_KEY=dev-admin-key           # the server's ADMIN_KEY

ADMIN=$(curl -s -X POST $BASE/auth/token -H "X-Admin-Key: $ADMIN_KEY" \
  -H 'Content-Type: application/json' -d '{"user_id":"ops","role":"admin"}' | jq -r .token)
ALICE=$(curl -s -X POST $BASE/auth/token -H 'Content-Type: application/json' \
  -d '{"user_id":"alice"}' | jq -r .token)
BOB=$(curl -s -X POST $BASE/auth/token -H 'Content-Type: application/json' \
  -d '{"user_id":"bob"}' | jq -r .token)
# a token response (alice's):
# {"expires_at":"2026-10-05T18:00:59.440299213Z","role":"user","token":"eyJhbGciOiJIUzI1NiIs...","token_type":"Bearer","user_id":"alice"}

SHOW=$(curl -s -X POST $BASE/shows -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"Docs demo","seats":["A1","A2","A3","A4","A5","A6"],"price_paise":25000,"per_user_limit":4}' \
  | jq -r .id)
# 201, Location: /shows/<id>
# {"id":"01a10813-8508-77eb-b8b5-84052b0ef61b","name":"Docs demo","price_paise":25000,"per_user_limit":4,
#  "total_seats":6,"created_at":"2026-10-04T18:00:59.401603Z","available":6,"held":0,"confirmed":0,
#  "invariant_ok":true,"seats":[{"label":"A1","status":"available"}, ...]}
```

Reserve two seats:

```bash
curl -si -X POST $BASE/shows/$SHOW/reserve -H "Authorization: Bearer $ALICE" \
  -H 'Idempotency-Key: alice-1' -H 'Content-Type: application/json' -d '{"seats":["A2","A1"]}'
```

```text
HTTP/1.1 201 Created
Idempotent-Replayed: false
Location: /reservations/01a10813-8559-75d3-9227-2d613d00b4b7
X-Request-Id: f3c22cba83f4c9a6167c6c00

{"reservation_id":"01a10813-8559-75d3-9227-2d613d00b4b7","show_id":"01a10813-8508-77eb-b8b5-84052b0ef61b","user_id":"alice","seats":["A1","A2"],"amount_paise":50000,"status":"confirmed","created_at":"2026-10-04T18:00:59.481313Z"}
```

Re-sending the same key is safe, in any seat order. It returns the same reservation with
`Idempotent-Replayed: true`, so it is also a way to capture the id:

```bash
RES=$(curl -s -X POST $BASE/shows/$SHOW/reserve -H "Authorization: Bearer $ALICE" \
  -H 'Idempotency-Key: alice-1' -H 'Content-Type: application/json' -d '{"seats":["A1","A2"]}' \
  | jq -r .reservation_id)
```

Declines are 409 with a machine-readable `error`, and nothing is booked:

```bash
# same key, different seats
curl -s -X POST $BASE/shows/$SHOW/reserve -H "Authorization: Bearer $ALICE" \
  -H 'Idempotency-Key: alice-1' -H 'Content-Type: application/json' -d '{"seats":["A3"]}'
# {"error":"idempotency_key_reused","message":"idempotency key was already used with a different request","request_id":"92accc26b60c2a59baeec45f"}

# A2 is taken, so bob gets neither seat (all-or-nothing)
curl -s -X POST $BASE/shows/$SHOW/reserve -H "Authorization: Bearer $BOB" \
  -H 'Content-Type: application/json' -d '{"seats":["A2","A3"],"idempotency_key":"bob-1"}'
# {"error":"seat_taken","message":"seat(s) not available: A2","request_id":"76047c5941b6f24764230c7b","unavailable_seats":["A2"]}

# alice holds 2, asks for 3 more, limit 4
curl -s -X POST $BASE/shows/$SHOW/reserve -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' -d '{"seats":["A3","A4","A5"],"idempotency_key":"alice-2"}'
# {"error":"per_user_limit","held":2,"limit":4,"message":"per-user limit of 4 seats for this show would be exceeded","request_id":"23a8f374b213401e4739c75a","requested":3}
```

Read and cancel (owner only; bob gets 403 `forbidden` on both):

```bash
curl -s $BASE/reservations/$RES -H "Authorization: Bearer $ALICE"
curl -s -X POST $BASE/reservations/$RES/cancel -H "Authorization: Bearer $ALICE"
# {"reservation_id":"01a10813-8559-75d3-9227-2d613d00b4b7","show_id":"01a10813-8508-77eb-b8b5-84052b0ef61b","user_id":"alice","seats":["A1","A2"],"amount_paise":50000,"status":"cancelled","created_at":"2026-10-04T18:00:59.481313Z","cancelled_at":"2026-10-04T18:00:59.612115Z"}
```

After a cancel, replaying `alice-1` returns the cancelled reservation (201, replayed). It does
not book again.

```bash
curl -s "$BASE/shows/$SHOW?seats=false"
curl -s $BASE/shows/$SHOW/audit | jq '{ok, checks: [.checks[] | {name, ok}]}'
```

### Status codes

Every error body is `{"error": "<code>", "message": "...", "request_id": "..."}`, plus the
extra fields listed here.

| Status | `error` | When |
|---|---|---|
| 200 | | reads, token, cancel (also when already cancelled) |
| 201 | | show created; reservation created (`Idempotent-Replayed: false`) or replayed (`true`) |
| 400 | `invalid_request` | bad JSON or a float where an integer is required; empty or duplicate seats; unknown seats (`unknown_seats`); missing key, or header and body keys differ; bad `user_id` or `role` |
| 401 | `unauthorized` | missing, invalid or expired bearer token (`WWW-Authenticate: Bearer`) |
| 403 | `forbidden` | non-admin creating a show; non-owner cancel or read; admin role without a valid `X-Admin-Key` |
| 404 | `show_not_found`, `reservation_not_found` | unknown or malformed id |
| 409 | `seat_taken` | a requested seat is not available (`unavailable_seats`) |
| 409 | `per_user_limit` | the show's limit would be exceeded (`limit`, `requested`, and `held` when known) |
| 409 | `idempotency_key_reused` | same key, different seats |
| 413 | `body_too_large` | body over the size limit (4 KiB for tokens, 64 KiB for reserve, 4 MiB for a show) |
| 499 | (no body) | the client went away first; logged, and kept out of the 5xx metrics |
| 503 | `not_ready`, `unavailable` | booting (`Retry-After: 2`); database unreachable, overloaded or timed out (`Retry-After: 1`). Retry with the same key |
| 500 | `internal_error` | a bug; panics are recovered and counted in `http_panics_total` |

## Configuration

All settings are environment variables. The server checks them at boot and refuses to start
with every problem listed at once: unparsable values, and out-of-range ones such as
`REQUEST_TIMEOUT=0` (which would otherwise answer 503 to every request).

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | required | Postgres URL |
| `PORT` | `8080` | listen port |
| `APP_ENV` | `production` in the image, `development` in compose | in production `JWT_SECRET` (32+ chars) and `ADMIN_KEY` (16+ chars) are required; otherwise dev defaults apply |
| `JWT_SECRET`, `ADMIN_KEY` | dev-only defaults outside production | token signing key; key for minting admin tokens |
| `TOKEN_TTL` | `24h` | |
| `DB_MAX_CONNS` | `40` | main pool (2-500). A separate 3-connection ops pool serves `/readyz`, the seat gauges and the auditor |
| `AUDIT_INTERVAL` | `30s` | scheduled reconciliation of the newest shows; `0` disables it |
| `AUDIT_SHOWS` | `5` | how many of the newest shows the auditor checks |
| `RESERVE_PRECHECK` | `true` | read-only fast path; `false` sends every request through the transaction |
| `REQUEST_TIMEOUT` | `30s` | per business request, and the shutdown drain limit |
| `SHUTDOWN_DELAY` | `3s` | time between failing readiness and draining on SIGTERM |
| `PUBLIC_LOGS` | `true` | `false` makes `/logs` return 404 |
| `LOG_BUFFER_LINES` | `20000` | size of the `/logs` tail |
| `LOG_STDOUT_RATE` | `400` | info/debug lines per second to stdout; `0` means unlimited |
| `LOG_LEVEL` | `info` | |

## Deploy

### Live: one small VM on AWS EC2

The live service is a single t3.micro in ap-south-1 (2 burstable vCPUs with unlimited credits,
1 GiB RAM, Ubuntu 24.04) behind a fixed Elastic IP. Everything is in `deploy/ec2/`:

| File | Role |
|---|---|
| `bootstrap.sh` | one-time host setup: Docker and Compose, rotated container logs, 2 GiB swap, deeper accept queues |
| `docker-compose.yml` | Postgres 17 (sized for 1 GiB, not published, last in line for the OOM killer), the app (`DB_MAX_CONNS=24`, `GOMEMLIMIT=300MiB`), Caddy |
| `Caddyfile` | automatic HTTPS for `<ip-with-dashes>.sslip.io`, plain HTTP on the bare IP, upstream idle timeout below the app's |
| `deploy.sh` | cross-compiles the image for linux/amd64 on the dev machine, streams it with `docker save \| ssh docker load`, generates secrets on the VM on first deploy, waits for `/readyz`, copies `ADMIN_KEY` to the gitignored `.env.live` |

```bash
ssh ubuntu@<ip> 'bash -s' < deploy/ec2/bootstrap.sh   # once per VM
deploy/ec2/deploy.sh ubuntu@<ip>                       # every deploy, about a minute
```

The security group allows 80 and 443 from anywhere and 22 from one address. Every container
restarts on failure and on boot (`restart: unless-stopped`, Docker enabled at boot), so a cold
start needs no manual step. Tested: after applying 168 package updates I rebooted the VM, and
`/readyz` was green again about 20 s after the reboot command.

### Alternative: Railway (prepared, not used for the live URL)

`railway.json` is ready for a Railway deploy: Dockerfile build, deploy gated on `GET /readyz`
(120 s timeout), restart on failure (up to 10), 10 s overlap between deployments, and a 40 s
SIGTERM-to-SIGKILL window (the 3 s readiness delay plus the 30 s drain). Add a PostgreSQL
service in the same region and set `DATABASE_URL=${{Postgres.DATABASE_URL}}`, `JWT_SECRET` (32+
characters) and `ADMIN_KEY` (16+ characters); the image defaults to `APP_ENV=production`, so the
server refuses to start without the last two. Keep one replica so the burst's per-process
counter check stays exact.

## Code map

```text
cmd/server/              boot (listener first, migrations in the background), pools, graceful shutdown
cmd/burst/               load and correctness tool behind ./burst.sh
internal/booking/        system of record: every SQL statement (store.go), audit, show cache, concurrency tests
internal/db/             pgx pools with session timeouts; embedded migrations under an advisory lock
internal/db/migrations/  001_init.sql: the schema, whose constraints carry the invariants
internal/httpapi/        routes (server.go), handlers, status and error mapping (respond.go)
internal/auth/           HS256 JWT issue and verify
internal/obs/            Prometheus metrics, JSON logging, /logs ring buffer, stdout rate limiter
internal/config/         environment configuration
```

Where to change things:

- **New endpoint:** route in `internal/httpapi/server.go`, handler in `handlers.go`, error
  mapping in `respond.go`.
- **New rule or query:** a SQL constant and a method in `internal/booking/store.go`; types and
  errors in `types.go`.
- **Schema change:** add `internal/db/migrations/002_<name>.sql`. It is embedded and applied
  once on boot, in filename order, under an advisory lock.
- **New decline reason:** `DeclineReason` in `types.go`, pre-create its series in
  `internal/obs/metrics.go`, and add any extra 409 fields in `writeDecline` (`respond.go`).
- **Tests:** `internal/booking/store_test.go` runs against real Postgres (`make test`). Wrap a
  new concurrency test in `bothModes` so it runs with the precheck on and off.
