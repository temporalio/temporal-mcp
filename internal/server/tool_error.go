// toolError, the structured error a tool returns, the helpers that build one for a target
// that is unknown or not authorized or for a failed Temporal call, and itemError, which
// decides whether an error reading one item of a list fails the whole call.

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

	"github.com/temporalio/temporal-operator-mcp/internal/config"
)

// The names of the servers a tool calls, as error messages give them.
const (
	temporalServer = "the Temporal server"
	cloudOpsAPI    = "the Temporal Cloud Ops API at saas-api.tmprl.cloud"
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

func temporalError(err error, t *config.Target, p *peer.Peer, server string) error {
	e := &toolError{Code: "temporal_unavailable", Message: rpcErrorMessage(err, t, p, server)}
	switch serviceerror.ToStatus(err).Code() {
	case codes.NotFound:
		if !errors.As(err, new(*serviceerror.NamespaceNotFound)) {
			e.Code = "not_found"
		}
	case codes.InvalidArgument:
		e.Code = "invalid_argument"
	case codes.DeadlineExceeded:
		e.Code, e.Retryable = "timeout", true
	case codes.ResourceExhausted:
		e.Code, e.Retryable = "rate_limited", true
	case codes.Unavailable:
		e.Retryable = true
	}
	return e
}

// itemError returns err, from reading item i of the list argument named list, as that item's error if it is a
// not_found or invalid_argument toolError, and otherwise as the error that fails the whole call, prefixing a
// toolError's message with the item's path, such as workflows[3].
func itemError(err error, list string, i int) (*toolError, error) {
	var te *toolError
	if !errors.As(err, &te) {
		return nil, err
	}
	if te.Code == "not_found" || te.Code == "invalid_argument" {
		return te, nil
	}
	te.Message = fmt.Sprintf("%s[%d]: %s", list, i, te.Message)
	return nil, err
}
