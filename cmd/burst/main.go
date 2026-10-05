// Command burst reproduces an on-sale stampede against a running service and then proves,
// from the client side, that it behaved correctly.
//
//	go run ./cmd/burst [flags] <BASE_URL>      (or ./burst.sh <BASE_URL>)
//
// Phases:
//  1. wait for /readyz (survives a cold start), create a fresh show, mint user tokens
//  2. stampede: N reserve requests released through one gate at C concurrency. Part of the
//     traffic storms a few hot seats; the rest picks front-biased seats (some 2-seat
//     requests); some requests are re-sent with the same idempotency key; some carry a
//     spoofed user_id in the body. A poller checks the invariant during the burst.
//  3. targeted probes: per-user limit (10 parallel vs limit), same key + different seats,
//     exact replays, non-owner cancel, owner cancel + rebook, spoofed identity
//  4. reconciliation: client ledger vs GET /shows/{id} (seat by seat) vs /metrics vs audit
//
// Exit status is 0 only if every check passes.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// probeSeats is how many seats of the last row the probes use (see probes).
const probeSeats = 22

type config struct {
	baseURL     string
	adminKey    string
	requests    int
	concurrency int
	users       int
	rows, cols  int
	hotSeats    int
	hotShare    float64
	retryShare  float64
	multiShare  float64
	spoofShare  float64
	limit       int
	price       int64
	timeout     time.Duration
	readyWait   time.Duration
	seed        uint64
	out         string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.adminKey, "admin-key", envOr("ADMIN_KEY", "dev-admin-key"), "admin key used to create the show (env ADMIN_KEY)")
	flag.IntVar(&c.requests, "requests", 20000, "reserve requests in the stampede (including same-key retries)")
	flag.IntVar(&c.concurrency, "concurrency", 1000, "requests in flight at once (one connection each)")
	flag.IntVar(&c.users, "users", 5000, "distinct buyers")
	flag.IntVar(&c.rows, "rows", 20, "rows in the hall (A..), the last row is kept free for probes")
	flag.IntVar(&c.cols, "cols", 50, "seats per row")
	flag.IntVar(&c.hotSeats, "hot-seats", 5, "number of hot seats everybody fights over")
	flag.Float64Var(&c.hotShare, "hot-share", 0.5, "share of requests aimed at the hot seats")
	flag.Float64Var(&c.retryShare, "retry-share", 0.1, "share of requests that re-send an earlier request with the same idempotency key")
	flag.Float64Var(&c.multiShare, "multi-share", 0.2, "share of non-hot requests asking for 2 adjacent seats")
	flag.Float64Var(&c.spoofShare, "spoof-share", 0.05, "share of requests carrying a spoofed user_id in the body")
	flag.IntVar(&c.limit, "limit", 4, "per_user_limit for the show")
	flag.Int64Var(&c.price, "price-paise", 25000, "seat price in paise")
	flag.DurationVar(&c.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.DurationVar(&c.readyWait, "ready-wait", 3*time.Minute, "how long to wait for /readyz (cold starts)")
	flag.Uint64Var(&c.seed, "seed", uint64(time.Now().UnixNano()), "random seed for the traffic plan")
	flag.StringVar(&c.out, "out", "", "optional path for a JSON summary")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: burst [flags] <BASE_URL>")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	c.baseURL = strings.TrimRight(flag.Arg(0), "/")
	// The last row holds the probe seats: 10 for the limit probe, 10 for key reuse, 2 more.
	if c.rows < 3 || c.rows > 26 || c.cols < probeSeats || c.hotSeats < 1 || c.hotSeats > c.cols {
		fmt.Fprintf(os.Stderr, "need 3 <= rows <= 26, cols >= %d, 1 <= hot-seats <= cols\n", probeSeats)
		os.Exit(2)
	}
	return c
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ---------------------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------------------

type client struct {
	base  string
	http  *http.Client
	runID string
	seq   atomic.Int64
}

type response struct {
	status   int
	body     []byte
	replayed bool
	latency  time.Duration
	err      error
}

func newClient(base string, concurrency int, runID string) *client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        concurrency + 64,
		MaxIdleConnsPerHost: concurrency + 64,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 20 * time.Second,
		// HTTP/1.1 only: every in-flight request gets its own connection, like distinct
		// buyers, instead of being multiplexed (and throttled) over one HTTP/2 connection.
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &client{base: base, http: &http.Client{Transport: tr}, runID: runID}
}

func (c *client) do(ctx context.Context, method, path, token string, hdr map[string]string, body any) response {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return response{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", fmt.Sprintf("burst-%s-%d", c.runID, c.seq.Add(1)))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return response{err: err, latency: time.Since(start)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, body: data, replayed: resp.Header.Get("Idempotent-Replayed") == "true",
		latency: time.Since(start), err: err}
}

func (c *client) call(method, path, token string, hdr map[string]string, body any, timeout time.Duration) response {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.do(ctx, method, path, token, hdr, body)
}

// ---------------------------------------------------------------------------------------
// API shapes
// ---------------------------------------------------------------------------------------

type reservation struct {
	ID          string   `json:"reservation_id"`
	ShowID      string   `json:"show_id"`
	UserID      string   `json:"user_id"`
	Seats       []string `json:"seats"`
	AmountPaise int64    `json:"amount_paise"`
	Status      string   `json:"status"`
}

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type showState struct {
	ID           string `json:"id"`
	TotalSeats   int    `json:"total_seats"`
	PerUserLimit int    `json:"per_user_limit"`
	Available    int    `json:"available"`
	Held         int    `json:"held"`
	Confirmed    int    `json:"confirmed"`
	InvariantOK  bool   `json:"invariant_ok"`
	Seats        []struct {
		Label  string `json:"label"`
		Status string `json:"status"`
	} `json:"seats"`
}

type auditReport struct {
	OK     bool `json:"ok"`
	Checks []struct {
		Name   string `json:"name"`
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
	} `json:"checks"`
}

// ---------------------------------------------------------------------------------------
// Traffic plan
// ---------------------------------------------------------------------------------------

type job struct {
	idx     int
	user    int
	seats   []string
	key     string
	hot     string // the hot seat this job storms, if any
	spoof   bool
	retryOf int // index of the original job when this is a same-key retry, else -1
}

type result struct {
	job      *job
	status   int // final status after transport-level retries
	replayed bool
	res      reservation
	errCode  string
	latency  []time.Duration
	attempts int
	saw5xx   int
	netErrs  int
	lastErr  error
}

type burst struct {
	cfg     config
	c       *client
	runID   string
	showID  string
	admin   string
	userIDs []string
	tokens  []string
	hot     []string
	free    []string // last row: never targeted by the stampede, used by the probes

	// client-side ledger of who holds what, built only from what the API told us
	ledgerMu sync.Mutex
	ledger   map[string]string // seat -> reservation id
	owner    map[string]string // reservation id -> user id
	conflict []string          // seats the API reported as sold to two reservations

	fresh, replays int
	declined       map[string]int
	cancelled      int
	saw5xx, netErr int

	checks []check
}

type check struct {
	name   string
	status string // PASS, FAIL, WARN
	detail string
}

func (b *burst) pass(name, format string, a ...any) { b.add(name, "PASS", format, a...) }
func (b *burst) fail(name, format string, a ...any) { b.add(name, "FAIL", format, a...) }
func (b *burst) warn(name, format string, a ...any) { b.add(name, "WARN", format, a...) }
func (b *burst) add(name, status, format string, a ...any) {
	b.checks = append(b.checks, check{name: name, status: status, detail: fmt.Sprintf(format, a...)})
}
func (b *burst) expect(ok bool, name, format string, a ...any) {
	if ok {
		b.pass(name, format, a...)
	} else {
		b.fail(name, format, a...)
	}
}

func seatLabel(row, col int) string { return string(rune('A'+row)) + strconv.Itoa(col+1) }

func (b *burst) hotSet() map[string]bool {
	m := map[string]bool{}
	for _, h := range b.hot {
		m[h] = true
	}
	return m
}

// frontBiasedSeat picks a seat with probability falling off from the front rows and
// the centre columns, excluding hot seats and the probe row.
func (b *burst) frontBiasedSeat(r *mrand.Rand, hot map[string]bool, width int) (row, col int) {
	for {
		row = int(r.ExpFloat64()*float64(b.cfg.rows-1)/4) % (b.cfg.rows - 1)
		col = int(r.NormFloat64()*float64(b.cfg.cols)/5 + float64(b.cfg.cols)/2)
		if col < 0 || col+width > b.cfg.cols {
			continue
		}
		ok := true
		for w := range width {
			if hot[seatLabel(row, col+w)] {
				ok = false
			}
		}
		if ok {
			return row, col
		}
	}
}

func (b *burst) plan() []*job {
	cfg := b.cfg
	r := mrand.New(mrand.NewPCG(cfg.seed, cfg.seed^0x9e3779b97f4a7c15))
	hot := b.hotSet()
	nRetry := int(float64(cfg.requests) * cfg.retryShare)
	nPrimary := cfg.requests - nRetry
	nHot := int(float64(nPrimary) * cfg.hotShare)

	jobs := make([]*job, 0, cfg.requests)
	for i := range nPrimary {
		j := &job{idx: i, user: r.IntN(cfg.users), key: fmt.Sprintf("%s-k%d", b.runID, i), retryOf: -1}
		switch {
		case i < nHot:
			j.hot = b.hot[i%len(b.hot)]
			j.seats = []string{j.hot}
		case r.Float64() < cfg.multiShare:
			row, col := b.frontBiasedSeat(r, hot, 2)
			j.seats = []string{seatLabel(row, col), seatLabel(row, col+1)}
		default:
			row, col := b.frontBiasedSeat(r, hot, 1)
			j.seats = []string{seatLabel(row, col)}
		}
		j.spoof = r.Float64() < cfg.spoofShare
		jobs = append(jobs, j)
	}
	for range nRetry {
		orig := jobs[r.IntN(nPrimary)]
		dup := *orig
		dup.idx, dup.retryOf = len(jobs), orig.idx
		jobs = append(jobs, &dup)
	}
	r.Shuffle(len(jobs), func(a, c int) { jobs[a], jobs[c] = jobs[c], jobs[a] })
	return jobs
}

// ---------------------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	var rid [4]byte
	_, _ = rand.Read(rid[:])
	runID := hex.EncodeToString(rid[:])
	b := &burst{cfg: cfg, runID: runID, c: newClient(cfg.baseURL, cfg.concurrency, runID),
		ledger: map[string]string{}, owner: map[string]string{}, declined: map[string]int{}}
	if err := b.run(); err != nil {
		fmt.Fprintln(os.Stderr, "\nburst aborted:", err)
		os.Exit(2)
	}
	if b.report() {
		os.Exit(0)
	}
	os.Exit(1)
}

func (b *burst) run() error {
	cfg := b.cfg
	fmt.Printf("== seat-reservation burst %s -> %s (seed %d)\n", b.runID, cfg.baseURL, cfg.seed)

	// 1. readiness (cold start)
	start := time.Now()
	for {
		r := b.c.call("GET", "/readyz", "", nil, nil, 10*time.Second)
		if r.err == nil && r.status == 200 {
			break
		}
		if time.Since(start) > cfg.readyWait {
			return fmt.Errorf("service not ready after %s (last: status=%d err=%v)", cfg.readyWait, r.status, r.err)
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Printf("ready after %s\n", time.Since(start).Round(time.Millisecond))

	// 2. admin token + fresh show
	tok := b.c.call("POST", "/auth/token", "", map[string]string{"X-Admin-Key": cfg.adminKey},
		map[string]any{"user_id": "burst-admin", "role": "admin"}, 30*time.Second)
	if tok.err != nil || tok.status != 200 {
		return fmt.Errorf("admin token: status=%d err=%v body=%s (set -admin-key / ADMIN_KEY)", tok.status, tok.err, tok.body)
	}
	var t struct{ Token string }
	_ = json.Unmarshal(tok.body, &t)
	b.admin = t.Token

	var seats []string
	for row := range cfg.rows {
		for col := range cfg.cols {
			seats = append(seats, seatLabel(row, col))
		}
	}
	mid := cfg.cols / 2
	for k := 0; len(b.hot) < cfg.hotSeats; k++ { // spiral out from the centre of row A (50 cols: A26 A25 A27 A24 A28)
		off := (k + 1) / 2
		if k%2 == 1 {
			off = -off
		}
		b.hot = append(b.hot, seatLabel(0, mid+off))
	}
	for col := range cfg.cols {
		b.free = append(b.free, seatLabel(cfg.rows-1, col))
	}
	cr := b.c.call("POST", "/shows", b.admin, nil, map[string]any{
		"name": "burst-" + b.runID, "seats": seats, "price_paise": cfg.price, "per_user_limit": cfg.limit,
	}, 60*time.Second)
	if cr.err != nil || cr.status != 201 {
		return fmt.Errorf("create show: status=%d err=%v body=%.300s", cr.status, cr.err, cr.body)
	}
	var created showState
	_ = json.Unmarshal(cr.body, &created)
	b.showID = created.ID
	fmt.Printf("show %s: %d seats (%d rows x %d), limit %d, hot seats %v\n",
		b.showID, created.TotalSeats, cfg.rows, cfg.cols, cfg.limit, b.hot)

	// 3. user tokens
	b.userIDs = make([]string, cfg.users)
	b.tokens = make([]string, cfg.users)
	if err := parallel(cfg.users, min(cfg.concurrency, 128), func(i int) error {
		b.userIDs[i] = fmt.Sprintf("burst-%s-u%d", b.runID, i)
		r := b.c.call("POST", "/auth/token", "", nil, map[string]any{"user_id": b.userIDs[i]}, 30*time.Second)
		if r.err != nil || r.status != 200 {
			return fmt.Errorf("user token %d: status=%d err=%v", i, r.status, r.err)
		}
		var t struct{ Token string }
		_ = json.Unmarshal(r.body, &t)
		b.tokens[i] = t.Token
		return nil
	}); err != nil {
		return err
	}
	fmt.Printf("minted %d user tokens\n", cfg.users)

	before, err := b.scrape()
	if err != nil {
		return fmt.Errorf("metrics before: %w", err)
	}

	// 4. stampede
	jobs := b.plan()
	results, elapsed, poll := b.stampede(jobs)
	for _, r := range results {
		b.record(r)
	}
	b.checkStampede(results, poll, elapsed)

	// 5. targeted probes
	b.probes(results)

	// 6. reconciliation
	b.reconcile(before)
	return nil
}

func parallel(n, workers int, fn func(i int) error) error {
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	idx := make(chan int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if err := fn(i); err != nil {
					once.Do(func() { first = err })
				}
			}
		}()
	}
	for i := range n {
		idx <- i
	}
	close(idx)
	wg.Wait()
	return first
}

type pollStats struct {
	polls, errors, violations, decreases, lastConfirmed int
}

func (b *burst) stampede(jobs []*job) ([]*result, time.Duration, pollStats) {
	cfg := b.cfg
	// Warm one connection per worker so the stampede starts on open sockets and the first
	// wave really lands within the same instant.
	_ = parallel(cfg.concurrency, cfg.concurrency, func(int) error {
		b.c.call("GET", "/healthz", "", nil, nil, 20*time.Second)
		return nil
	})

	fmt.Printf("stampede: %d requests, %d concurrent, %d users ...\n", len(jobs), cfg.concurrency, cfg.users)
	results := make([]*result, len(jobs))
	work := make(chan *job, len(jobs))
	for _, j := range jobs {
		work <- j
	}
	close(work)

	stopPoll := make(chan struct{})
	pollDone := make(chan pollStats)
	go b.pollInvariant(stopPoll, pollDone)

	gate := make(chan struct{})
	var wg sync.WaitGroup
	for range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			for j := range work {
				results[j.idx] = b.reserve(j)
			}
		}()
	}
	start := time.Now()
	close(gate)
	wg.Wait()
	elapsed := time.Since(start)
	close(stopPoll)
	return results, elapsed, <-pollDone
}

// reserve sends one job. Transport errors and 5xx are retried with the SAME idempotency key,
// exactly as a real client should: if the first attempt committed, the retry replays it.
func (b *burst) reserve(j *job) *result {
	body := map[string]any{"seats": j.seats, "idempotency_key": j.key}
	if j.spoof {
		body["user_id"] = "spoofed-victim"
	}
	res := &result{job: j}
	for attempt := 1; attempt <= 4; attempt++ {
		res.attempts = attempt
		r := b.c.call("POST", "/shows/"+b.showID+"/reserve", b.tokens[j.user],
			map[string]string{"Idempotency-Key": j.key}, body, b.cfg.timeout)
		res.latency = append(res.latency, r.latency)
		if r.err != nil {
			res.netErrs++
			res.lastErr = r.err
			time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
			continue
		}
		if r.status >= 500 {
			res.saw5xx++
			res.status = r.status
			time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
			continue
		}
		res.status, res.replayed = r.status, r.replayed
		if r.status == 201 {
			_ = json.Unmarshal(r.body, &res.res)
		} else {
			var e apiError
			_ = json.Unmarshal(r.body, &e)
			res.errCode = e.Error
		}
		return res
	}
	return res
}

func (b *burst) pollInvariant(stop <-chan struct{}, done chan<- pollStats) {
	var st pollStats
	poll := func() {
		s, err := b.show(false)
		if err != nil {
			st.errors++
			return
		}
		st.polls++
		if s.Available+s.Held+s.Confirmed != s.TotalSeats {
			st.violations++
		}
		if s.Confirmed < st.lastConfirmed { // nothing is cancelled during the stampede
			st.decreases++
		}
		st.lastConfirmed = s.Confirmed
	}
	// Poll at the start and at the end as well as on every tick, so even a stampede shorter
	// than one tick (a small local run) is sampled.
	poll()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			poll()
			done <- st
			return
		case <-t.C:
			poll()
		}
	}
}

func (b *burst) show(withSeats bool) (*showState, error) {
	path := "/shows/" + b.showID
	if !withSeats {
		path += "?seats=false"
	}
	r := b.c.call("GET", path, "", nil, nil, 30*time.Second)
	if r.err != nil || r.status != 200 {
		return nil, fmt.Errorf("GET %s: status=%d err=%v", path, r.status, r.err)
	}
	var s showState
	return &s, json.Unmarshal(r.body, &s)
}

// record folds one API outcome into the ledger and the counters.
func (b *burst) record(r *result) {
	b.saw5xx += r.saw5xx
	if r.status == 0 {
		b.netErr++
		return
	}
	switch {
	case r.status == 201 && r.replayed:
		b.replays++
	case r.status == 201:
		b.fresh++
	case r.status == 409:
		b.declined[r.errCode]++
	}
	if r.status != 201 {
		return
	}
	b.ledgerMu.Lock()
	defer b.ledgerMu.Unlock()
	if r.res.Status != "confirmed" {
		return
	}
	b.owner[r.res.ID] = r.res.UserID
	for _, s := range r.res.Seats {
		if prev, ok := b.ledger[s]; ok && prev != r.res.ID {
			b.conflict = append(b.conflict, s)
		}
		b.ledger[s] = r.res.ID
	}
}

func (b *burst) release(res reservation) {
	b.ledgerMu.Lock()
	defer b.ledgerMu.Unlock()
	for _, s := range res.Seats {
		if b.ledger[s] == res.ID {
			delete(b.ledger, s)
		}
	}
	delete(b.owner, res.ID)
}

func (b *burst) checkStampede(results []*result, poll pollStats, elapsed time.Duration) {
	cfg := b.cfg
	var lat []time.Duration
	for _, r := range results {
		lat = append(lat, r.latency...)
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(float64(len(lat))*p))].Round(time.Millisecond)
	}
	fmt.Printf("stampede done in %s (%.0f req/s); latency p50 %s p95 %s p99 %s max %s\n",
		elapsed.Round(time.Millisecond), float64(len(results))/elapsed.Seconds(),
		pct(.50), pct(.95), pct(.99), pct(1))

	b.expect(len(b.conflict) == 0, "no seat sold twice",
		"%d seats appeared in two different reservations across %d confirmed responses", len(b.conflict), b.fresh+b.replays)
	b.expect(b.saw5xx == 0, "zero 5xx", "%d 5xx responses across the stampede", b.saw5xx)
	b.expect(b.netErr == 0, "zero transport errors (after same-key retries)", "%d requests never got a response", b.netErr)

	// Hot seats: exactly one winning reservation each; every other attempt is a 409.
	hotOK := true
	var hotDetail []string
	for _, h := range b.hot {
		winners := map[string]bool{}
		attempts, non409 := 0, 0
		for _, r := range results {
			if r.job.hot != h {
				continue
			}
			attempts++
			switch r.status {
			case 201:
				winners[r.res.ID] = true
			case 409:
			default:
				non409++
			}
		}
		if len(winners) != 1 || non409 != 0 {
			hotOK = false
		}
		hotDetail = append(hotDetail, fmt.Sprintf("%s: %d attempts, %d winner, %d non-409", h, attempts, len(winners), non409))
	}
	b.expect(hotOK, "hot seats: exactly one winner each, everyone else 409", "%s", strings.Join(hotDetail, "; "))

	// Same key -> same reservation; at most one fresh 201 per key.
	type group struct {
		ids   map[string]bool
		fresh int
		n     int
	}
	groups := map[string]*group{}
	for _, r := range results {
		k := b.userIDs[r.job.user] + "|" + r.job.key
		g := groups[k]
		if g == nil {
			g = &group{ids: map[string]bool{}}
			groups[k] = g
		}
		g.n++
		if r.status == 201 {
			g.ids[r.res.ID] = true
			if !r.replayed {
				g.fresh++
			}
		}
	}
	retried, badKeys := 0, 0
	for _, g := range groups {
		if g.n > 1 {
			retried++
		}
		if len(g.ids) > 1 || g.fresh > 1 {
			badKeys++
		}
	}
	b.expect(badKeys == 0, "idempotent retries move nothing extra",
		"%d keys sent more than once (concurrently or later); %d keys produced more than one reservation; %d replays served",
		retried, badKeys, b.replays)

	// Identity: every reservation belongs to the token's user, spoofed body or not.
	spoofed, wrongOwner := 0, 0
	for _, r := range results {
		if r.status != 201 {
			continue
		}
		if r.job.spoof {
			spoofed++
		}
		if r.res.UserID != b.userIDs[r.job.user] {
			wrongOwner++
		}
	}
	b.expect(wrongOwner == 0, "identity comes from the token",
		"%d successful requests carried a spoofed body user_id; %d reservations had the wrong owner", spoofed, wrongOwner)

	// Per-user limit across the whole stampede.
	perUser := map[string]int{}
	b.ledgerMu.Lock()
	for _, rid := range b.ledger {
		perUser[b.owner[rid]]++
	}
	b.ledgerMu.Unlock()
	over, maxHeld := 0, 0
	for _, n := range perUser {
		maxHeld = max(maxHeld, n)
		if n > cfg.limit {
			over++
		}
	}
	b.expect(over == 0, "per-user limit during the stampede",
		"max seats held by one user = %d (limit %d); %d users over", maxHeld, cfg.limit, over)

	b.expect(poll.polls > 0 && poll.violations == 0 && poll.decreases == 0,
		"invariant held during the burst",
		"%d polls of GET /shows/{id}: %d violations of available+held+confirmed==total, %d decreases (%d poll errors)",
		poll.polls, poll.violations, poll.decreases, poll.errors)
}

// probes are small, targeted scenarios run on the probe row (untouched by the stampede).
func (b *burst) probes(results []*result) {
	cfg := b.cfg
	token := func(user string) string {
		r := b.c.call("POST", "/auth/token", "", nil, map[string]any{"user_id": user}, 30*time.Second)
		var t struct{ Token string }
		_ = json.Unmarshal(r.body, &t)
		return t.Token
	}
	reserve := func(tok string, seats []string, key string, extra map[string]any) response {
		body := map[string]any{"seats": seats, "idempotency_key": key}
		for k, v := range extra {
			body[k] = v
		}
		return b.c.call("POST", "/shows/"+b.showID+"/reserve", tok, nil, body, cfg.timeout)
	}
	parse := func(r response) (reservation, string) {
		var res reservation
		var e apiError
		if r.status == 201 {
			_ = json.Unmarshal(r.body, &res)
		} else {
			_ = json.Unmarshal(r.body, &e)
		}
		return res, e.Error
	}
	track := func(r response) reservation {
		res, code := parse(r)
		b.record(&result{status: r.status, replayed: r.replayed, res: res, errCode: code})
		return res
	}
	free := b.free

	// Per-user limit: 10 parallel requests for 10 free seats, limit N -> exactly N succeed.
	limitTok := token("burst-" + b.runID + "-limit")
	n := min(10, len(free))
	codes := make([]response, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = reserve(limitTok, []string{free[i]}, fmt.Sprintf("limit-%d", i), nil)
		}()
	}
	wg.Wait()
	ok, limited := 0, 0
	for _, r := range codes {
		track(r)
		if r.status == 201 {
			ok++
		} else if _, code := parse(r); r.status == 409 && code == "per_user_limit" {
			limited++
		}
	}
	b.expect(ok == min(cfg.limit, n) && ok+limited == n, "per-user limit under 10 parallel requests",
		"%d confirmed, %d declined per_user_limit (limit %d)", ok, limited, cfg.limit)

	// Same key + different seats -> 409, nothing moves. Exact retry -> original reservation.
	var winners []*result
	for _, r := range results {
		if r.status == 201 && !r.replayed && r.res.Status == "confirmed" {
			winners = append(winners, r)
		}
	}
	probes := min(10, len(winners))
	reused, replayed := 0, 0
	for i := 0; i < probes; i++ {
		w := winners[i]
		tok := b.tokens[w.job.user]
		r := reserve(tok, []string{free[10+i%10]}, w.job.key, nil)
		if _, code := parse(r); r.status == 409 && code == "idempotency_key_reused" {
			reused++
		}
		track(r)
		r = reserve(tok, w.job.seats, w.job.key, nil)
		if res, _ := parse(r); r.status == 201 && r.replayed && res.ID == w.res.ID {
			replayed++
		}
		track(r)
	}
	b.expect(reused == probes, "same key + different seats -> 409", "%d/%d rejected with idempotency_key_reused", reused, probes)
	b.expect(replayed == probes, "exact retry returns the original reservation", "%d/%d replayed with the same reservation_id", replayed, probes)

	// Identity: a spoofed body user_id is ignored.
	spoofTok := token("burst-" + b.runID + "-spoofer")
	r := reserve(spoofTok, []string{free[20]}, "spoof-1", map[string]any{"user_id": "burst-" + b.runID + "-victim"})
	res := track(r)
	b.expect(r.status == 201 && res.UserID == "burst-"+b.runID+"-spoofer", "spoofed body user_id is ignored",
		"status %d, reservation owned by %q", r.status, res.UserID)

	// Cancel: non-owner is refused and nothing changes; owner cancels; seat is re-bookable.
	ownerTok := token("burst-" + b.runID + "-owner")
	attackerTok := token("burst-" + b.runID + "-attacker")
	booked := track(reserve(ownerTok, []string{free[21]}, "cancel-1", nil))
	att := b.c.call("POST", "/reservations/"+booked.ID+"/cancel", attackerTok, nil, nil, cfg.timeout)
	st, _ := b.show(true)
	stillConfirmed := st != nil && seatStatus(st, free[21]) == "confirmed"
	b.expect((att.status == 403 || att.status == 404) && stillConfirmed, "only the owner can cancel",
		"non-owner cancel -> %d; seat %s still confirmed: %v", att.status, free[21], stillConfirmed)
	own := b.c.call("POST", "/reservations/"+booked.ID+"/cancel", ownerTok, nil, nil, cfg.timeout)
	if own.status == 200 {
		b.release(booked)
		b.cancelled++
	}
	rebook := reserve(token("burst-"+b.runID+"-rebooker"), []string{free[21]}, "rebook-1", nil)
	track(rebook)
	b.expect(own.status == 200 && rebook.status == 201, "owner cancel makes the seat re-bookable",
		"owner cancel -> %d; rebook of %s -> %d", own.status, free[21], rebook.status)

	// A hot seat stays sold.
	late := reserve(token("burst-"+b.runID+"-late"), []string{b.hot[0]}, "late-1", nil)
	_, code := parse(late)
	track(late)
	b.expect(late.status == 409 && code == "seat_taken", "a hot seat stays sold after the storm",
		"late request for %s -> %d %s", b.hot[0], late.status, code)
}

func seatStatus(s *showState, label string) string {
	for _, seat := range s.Seats {
		if seat.Label == label {
			return seat.Status
		}
	}
	return ""
}

func (b *burst) reconcile(before map[string]float64) {
	st, err := b.show(true)
	if err != nil {
		b.fail("final show state", "%v", err)
		return
	}
	sum := st.Available + st.Held + st.Confirmed
	b.expect(sum == st.TotalSeats && st.InvariantOK, "final invariant",
		"available %d + held %d + confirmed %d = %d of %d seats", st.Available, st.Held, st.Confirmed, sum, st.TotalSeats)

	// Seat by seat: the API must confirm exactly the seats clients were told they got.
	b.ledgerMu.Lock()
	mismatch := 0
	for _, seat := range st.Seats {
		_, inLedger := b.ledger[seat.Label]
		if (seat.Status == "confirmed") != inLedger {
			mismatch++
		}
	}
	ledgerSize := len(b.ledger)
	b.ledgerMu.Unlock()
	b.expect(mismatch == 0 && ledgerSize == st.Confirmed, "API state == what clients were told",
		"%d seats confirmed by the API, %d in the client ledger, %d seat-level mismatches", st.Confirmed, ledgerSize, mismatch)

	after, err := b.scrape()
	if err != nil {
		b.fail("metrics", "%v", err)
		return
	}
	g := func(name string) float64 { return after[fmt.Sprintf(`%s{show_id="%s"}`, name, b.showID)] }
	b.expect(int(g("seats_available")) == st.Available && int(g("seats_confirmed")) == st.Confirmed &&
		int(g("seats_total")) == st.TotalSeats && g("seats_reconciled") == 1,
		"metrics gauges == API state",
		"seats_available %v, seats_confirmed %v, seats_total %v, seats_reconciled %v",
		g("seats_available"), g("seats_confirmed"), g("seats_total"), g("seats_reconciled"))

	// Counters are per process: these deltas match only with one replica and no other traffic.
	d := func(k string) int { return int(after[k] - before[k]) }
	var diffs []string
	cmp := func(label string, metric, observed int) {
		if metric != observed {
			diffs = append(diffs, fmt.Sprintf("%s metric %d != observed %d", label, metric, observed))
		}
	}
	cmp("confirmed", d("reservations_confirmed_total"), b.fresh)
	cmp("idempotent_replay", d(`reservations_declined_total{reason="idempotent_replay"}`), b.replays)
	for _, reason := range []string{"seat_taken", "per_user_limit", "idempotency_key_reused"} {
		cmp(reason, d(fmt.Sprintf(`reservations_declined_total{reason="%s"}`, reason)), b.declined[reason])
	}
	cmp("cancelled", d("reservations_cancelled_total"), b.cancelled)
	detail := fmt.Sprintf("confirmed +%d, replays +%d, seat_taken +%d, per_user_limit +%d, key_reused +%d, cancelled +%d",
		d("reservations_confirmed_total"), d(`reservations_declined_total{reason="idempotent_replay"}`),
		d(`reservations_declined_total{reason="seat_taken"}`), d(`reservations_declined_total{reason="per_user_limit"}`),
		d(`reservations_declined_total{reason="idempotency_key_reused"}`), d("reservations_cancelled_total"))
	if len(diffs) == 0 {
		b.pass("metrics counters == observed outcomes", "%s", detail)
	} else {
		b.warn("metrics counters == observed outcomes", "%s (other traffic or >1 replica?) %s", strings.Join(diffs, "; "), detail)
	}

	r := b.c.call("GET", "/shows/"+b.showID+"/audit", "", nil, nil, 60*time.Second)
	var rep auditReport
	_ = json.Unmarshal(r.body, &rep)
	var failed []string
	for _, c := range rep.Checks {
		if !c.OK {
			failed = append(failed, c.Name+": "+c.Detail)
		}
	}
	b.expect(r.status == 200 && rep.OK, "server-side audit (cross-table reconciliation)",
		"%d/%d checks ok %s", len(rep.Checks)-len(failed), len(rep.Checks), strings.Join(failed, "; "))
}

// scrape parses the Prometheus text format into "name{label="v",...}" -> value, plus
// per-reason sums of reservations_declined_total and per-show seat gauges keyed by show_id.
func (b *burst) scrape() (map[string]float64, error) {
	r := b.c.call("GET", "/metrics", "", nil, nil, 30*time.Second)
	if r.err != nil || r.status != 200 {
		return nil, fmt.Errorf("GET /metrics: status=%d err=%v", r.status, r.err)
	}
	out := map[string]float64{}
	for _, line := range strings.Split(string(r.body), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		series := line[:i]
		out[series] = v
		name, labels, _ := strings.Cut(series, "{")
		switch name {
		case "reservations_declined_total":
			if reason := labelValue(labels, "reason"); reason != "" {
				out[fmt.Sprintf(`%s{reason="%s"}`, name, reason)] += v
			}
		case "seats_available", "seats_held", "seats_confirmed", "seats_total", "seats_reconciled":
			out[fmt.Sprintf(`%s{show_id="%s"}`, name, labelValue(labels, "show_id"))] = v
		}
	}
	return out, nil
}

func labelValue(labels, key string) string {
	i := strings.Index(labels, key+`="`)
	if i < 0 {
		return ""
	}
	rest := labels[i+len(key)+2:]
	if j := strings.IndexByte(rest, '"'); j >= 0 {
		return rest[:j]
	}
	return ""
}

func (b *burst) report() bool {
	fmt.Println()
	fmt.Println("outcomes (stampede + probes)")
	fmt.Printf("  201 confirmed (new)          %7d\n", b.fresh)
	fmt.Printf("  201 idempotent replay        %7d\n", b.replays)
	reasons := make([]string, 0, len(b.declined))
	for k := range b.declined {
		reasons = append(reasons, k)
	}
	slices.Sort(reasons)
	for _, k := range reasons {
		fmt.Printf("  409 %-25s %7d\n", k, b.declined[k])
	}
	fmt.Printf("  5xx                          %7d\n", b.saw5xx)
	fmt.Printf("  no response (transport)      %7d\n", b.netErr)
	fmt.Println()
	fmt.Println("checks")
	ok := true
	for _, c := range b.checks {
		mark := map[string]string{"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}[c.status]
		fmt.Printf("  [%s] %s\n         %s\n", mark, c.name, c.detail)
		if c.status == "FAIL" {
			ok = false
		}
	}
	fmt.Println()
	fmt.Printf("show %s -- inspect: %s/shows/%s  %s/shows/%s/audit  %s/logs?q=%s\n",
		b.showID, b.cfg.baseURL, b.showID, b.cfg.baseURL, b.showID, b.cfg.baseURL, b.runID)
	if ok {
		fmt.Println("RESULT: PASS")
	} else {
		fmt.Println("RESULT: FAIL")
	}
	if b.cfg.out != "" {
		summary := map[string]any{"run_id": b.runID, "show_id": b.showID, "base_url": b.cfg.baseURL, "pass": ok,
			"confirmed": b.fresh, "replays": b.replays, "declined": b.declined, "5xx": b.saw5xx,
			"transport_errors": b.netErr, "checks": b.checks}
		if data, err := json.MarshalIndent(summary, "", "  "); err == nil {
			_ = os.WriteFile(b.cfg.out, data, 0o644)
		}
	}
	return ok
}
