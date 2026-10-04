package booking

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// maxCachedShows bounds memory. Shows are immutable, so a crude "reset when full" policy is
// safe: the worst case is one extra load from Postgres.
const maxCachedShows = 1000

// showCache keeps immutable show metadata (price, limit, label -> seat id) in memory, so the
// hot path needs no extra query to validate seats and compute the amount.
//
// singleflight collapses a stampede of first requests for a show that is not cached yet
// (e.g. right after a restart) into a single database load.
type showCache struct {
	mu    sync.RWMutex
	shows map[string]*Show
	group singleflight.Group
	load  func(ctx context.Context, id string) (*Show, error)
}

func newShowCache(load func(ctx context.Context, id string) (*Show, error)) *showCache {
	return &showCache{shows: make(map[string]*Show), load: load}
}

func (c *showCache) get(ctx context.Context, id string) (*Show, error) {
	c.mu.RLock()
	sh := c.shows[id]
	c.mu.RUnlock()
	if sh != nil {
		return sh, nil
	}
	v, err, _ := c.group.Do(id, func() (any, error) {
		// Detach from the first caller's cancellation: other callers share this load.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		sh, err := c.load(ctx, id)
		if err != nil {
			return nil, err
		}
		c.put(sh)
		return sh, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Show), nil
}

func (c *showCache) put(sh *Show) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.shows) >= maxCachedShows {
		c.shows = make(map[string]*Show)
	}
	c.shows[sh.ID] = sh
}
