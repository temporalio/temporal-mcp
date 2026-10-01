package server

import (
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
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
		{codes.Unavailable, "temporal_unavailable", true},
		{codes.PermissionDenied, "temporal_unavailable", false},
		{codes.Internal, "temporal_unavailable", false},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			var got *toolError
			if !errors.As(temporalError(status.Error(tt.code, "x"), &config.Target{Address: "a:7233"}, &peer.Peer{}), &got) {
				t.Fatal("not a tool error")
			}
			if got.Code != tt.want || got.Retryable != tt.retryable {
				t.Errorf("code = %s, retryable = %v, want %s, %v", got.Code, got.Retryable, tt.want, tt.retryable)
			}
		})
	}
}

func TestTemporalErrorNamespaceNotFound(t *testing.T) {
	var got *toolError
	if !errors.As(temporalError(serviceerror.NewNamespaceNotFound("ns1"), &config.Target{Address: "a:7233"}, &peer.Peer{}), &got) {
		t.Fatal("not a tool error")
	}
	want := toolError{Code: "temporal_unavailable", Message: "Namespace ns1 is not found.", Retryable: false}
	if *got != want {
		t.Errorf("error = %+v, want %+v", *got, want)
	}
}
