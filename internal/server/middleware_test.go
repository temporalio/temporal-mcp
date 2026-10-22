package server

import (
	"context"
	"errors"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-mcp/internal/config"
)

func TestShutdownCancellationResult(t *testing.T) {
	temporalCancelled := temporalError(status.Error(codes.Canceled, "context canceled"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer)
	tests := []struct {
		name          string
		shutdown      bool
		clientCancels bool
		handlerError  error
		code          string
		retryable     bool
	}{
		{"shutdown during raw cancellation", true, false, context.Canceled, "server_shutting_down", true},
		{"shutdown during Temporal cancellation", true, false, temporalCancelled, "server_shutting_down", true},
		{"client cancellation", false, true, temporalCancelled, "temporal_unavailable", false},
		{"client cancels before shutdown", true, true, temporalCancelled, "temporal_unavailable", false},
		{"unrelated failure during shutdown", true, false, errors.New("unrelated failure"), "internal", false},
		{"permission denied during shutdown", true, false, temporalError(status.Error(codes.PermissionDenied, "denied"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer), "temporal_unavailable", false},
		{"success during shutdown", true, false, nil, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stop, shutdown := context.WithCancel(t.Context())
			defer shutdown()
			client, cancelClient := context.WithCancel(t.Context())
			defer cancelClient()
			success := &mcp.CallToolResult{}
			handler := cancelWith(stop, stop)(normalizeToolCalls(func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
				if tt.clientCancels {
					cancelClient()
				}
				if tt.shutdown {
					shutdown()
				}
				<-ctx.Done()
				if tt.handlerError != nil {
					return errorResult(tt.handlerError), nil
				}
				return success, nil
			}))
			res, err := handler(client, "tools/call", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			if tt.handlerError == nil {
				if res != success {
					t.Errorf("result = %+v, want the successful result", res)
				}
				return
			}
			result, ok := res.(*mcp.CallToolResult)
			if !ok || !result.IsError {
				t.Fatalf("result = %+v, want a tool error", res)
			}
			var te *toolError
			if !errors.As(result.GetError(), &te) {
				t.Fatalf("error = %v, want a toolError", result.GetError())
			}
			if te.Code != tt.code || te.Retryable != tt.retryable {
				t.Errorf("error = %s, want code %s, retryable %v", te, tt.code, tt.retryable)
			}
		})
	}
}
