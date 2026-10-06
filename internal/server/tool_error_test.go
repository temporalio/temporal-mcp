package server

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
)

func TestTemporalError(t *testing.T) {
	tests := []struct {
		code      codes.Code
		want      string
		retryable bool
	}{
		{codes.NotFound, "not_found", false},
		{codes.InvalidArgument, "invalid_argument", false},
		{codes.DeadlineExceeded, "timeout", true},
		{codes.ResourceExhausted, "rate_limited", true},
		{codes.Unavailable, "temporal_unavailable", true},
		{codes.PermissionDenied, "temporal_unavailable", false},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			var got *toolError
			if !errors.As(temporalError(status.Error(tt.code, "x"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer), &got) {
				t.Fatal("not a tool error")
			}
			if got.Code != tt.want || got.Retryable != tt.retryable {
				t.Errorf("code = %s, retryable = %v, want %s, %v", got.Code, got.Retryable, tt.want, tt.retryable)
			}
		})
	}
}
