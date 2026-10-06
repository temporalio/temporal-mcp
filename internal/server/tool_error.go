// toolError, the structured error a tool returns, and the helpers that build one for a
// target that is unknown or not authorized, or for a failed Temporal call.

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"

	"github.com/temporalio/operator-mcp/internal/config"
)

type toolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	cause     error
}

func (e *toolError) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func lookupTarget(ctx context.Context, cfg *config.Config, name string) (*config.Target, error) {
	t, ok := cfg.Lookup(name)
	granted := ok && slices.Contains(toolTargets(ctx), t.Name)
	if rec := auditRecordFrom(ctx); rec != nil && ok {
		rec.denied = !granted
	}
	if !granted {
		return nil, &toolError{Code: "not_found", Message: fmt.Sprintf("unknown target %q", name)}
	}
	return t, nil
}

func temporalError(err error, t *config.Target, p *peer.Peer) error {
	e := &toolError{Code: "temporal_unavailable", Message: rpcErrorMessage(err, t, p)}
	switch serviceerror.ToStatus(err).Code() {
	case codes.NotFound:
		if !errors.As(err, new(*serviceerror.NamespaceNotFound)) {
			e.Code = "not_found"
		}
	case codes.InvalidArgument:
		e.Code = "invalid_argument"
	case codes.DeadlineExceeded:
		e.Code, e.Retryable = "timeout", true
	case codes.Unavailable:
		e.Retryable = true
	}
	return e
}
