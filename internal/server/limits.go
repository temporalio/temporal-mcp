// Limits on tool calls, each enforced by this replica alone: a deadline for each call, caps on the calls in progress on
// each target and on the replica, a cap on the size of a response, and a rate limit for each caller.

package server

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
)

var (
	toolCallTimeout        = 50 * time.Second
	maxToolCallsPerTarget  = 8
	maxToolCallsPerReplica = 32
	maxResponseBytes       = 4 << 20
	callerRate             = rate.Limit(5)
	callerBurst            = 30
)

type limits struct {
	cfg     *config.Config
	replica *semaphore.Weighted
	targets map[string]*semaphore.Weighted
	mu      sync.Mutex
	callers map[string]*rate.Limiter
}

func newLimits(cfg *config.Config) *limits {
	l := &limits{cfg: cfg, replica: semaphore.NewWeighted(int64(maxToolCallsPerReplica)), targets: map[string]*semaphore.Weighted{}, callers: map[string]*rate.Limiter{}}
	for _, t := range cfg.Targets {
		l.targets[t.Name] = semaphore.NewWeighted(int64(maxToolCallsPerTarget))
	}
	return l
}

// acquire turns call away if its caller is over its rate, and otherwise waits, until ctx ends at the latest, for a slot
// on the target it names, if ctx grants that target, and then for one on the replica, so that calls queued on a hung
// target hold no replica slot. It returns the function that frees the slots.
func (l *limits) acquire(ctx context.Context, call *mcp.CallToolRequest) (release func(), err error) {
	if wait := l.callerWait(call); wait > 0 {
		return nil, &toolError{Code: "rate_limited", Message: fmt.Sprintf("too many calls from this caller; retry in %v", (wait + time.Millisecond - 1).Truncate(time.Millisecond)), Retryable: true}
	}
	slots := []*semaphore.Weighted{l.replica}
	if name := callTarget(l.cfg, call); name != nil && slices.Contains(toolTargets(ctx), *name) {
		slots = []*semaphore.Weighted{l.targets[*name], l.replica}
	}
	var held []*semaphore.Weighted
	release = func() {
		for _, s := range held {
			s.Release(1)
		}
	}
	for _, s := range slots {
		if err := s.Acquire(ctx, 1); err != nil {
			release()
			return nil, &toolError{Code: "rate_limited", Message: "this server is busy with other calls; retry later", Retryable: true, cause: err}
		}
		held = append(held, s)
	}
	return release, nil
}

// callerWait counts call against its caller's rate and returns 0, or, if the caller is over its rate, returns how long
// it must wait and counts nothing. Callers without a token share one limit.
func (l *limits) callerWait(call *mcp.CallToolRequest) time.Duration {
	var caller string
	if call.Extra != nil && call.Extra.TokenInfo != nil {
		caller = call.Extra.TokenInfo.UserID
	}
	// The lock is held through Cancel, which cannot fully undo a reservation that another one followed.
	l.mu.Lock()
	defer l.mu.Unlock()
	limiter, ok := l.callers[caller]
	if !ok {
		limiter = rate.NewLimiter(callerRate, callerBurst)
		l.callers[caller] = limiter
	}
	r := limiter.Reserve()
	wait := r.Delay()
	if wait > 0 {
		r.Cancel()
	}
	return wait
}

// toolCallTimeoutError is the error of a call that cannot finish within toolCallTimeout.
func toolCallTimeoutError() *toolError {
	return &toolError{Code: "timeout", Message: fmt.Sprintf("the call did not finish within %v", toolCallTimeout), Retryable: true}
}

// deadlinePassed reports whether ctx's deadline has passed. It reads the clock rather than ctx.Err(), which is set only
// once ctx's timer fires, after a deadline error from a server may already have come back.
func deadlinePassed(ctx context.Context) bool {
	d, ok := ctx.Deadline()
	return ok && !time.Now().Before(d)
}
