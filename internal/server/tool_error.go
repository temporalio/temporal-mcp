package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"

	"github.com/temporalio/operator-mcp/internal/config"
)

type toolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *toolError) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func lookupTarget(cfg *config.Config, name string) (*config.Target, error) {
	t, ok := cfg.Lookup(name)
	if !ok {
		return nil, &toolError{Code: "not_found", Message: fmt.Sprintf("unknown target %q; list_available_targets lists the targets", name)}
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
