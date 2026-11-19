// Snapshots: what the server builds from one config. Each tool call keeps the snapshot it
// starts with until it ends, while Reload replaces the snapshot for the requests after it.

package server

import (
	"context"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

// A snapshot is a config with the clients and payload renderer for its targets, the names of its targets, and the
// search attribute names that its tool calls listed.
type snapshot struct {
	cfg      *config.Config
	clients  *temporal.Clients
	payloads *payload.Renderer
	targets  []string
	names    searchAttributeNames
	// holders counts the tool calls that hold the snapshot.
	holders sync.WaitGroup
}

func newSnapshot(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) *snapshot {
	sn := &snapshot{cfg: cfg, clients: clients, payloads: payloads, targets: make([]string, len(cfg.Targets))}
	for i, t := range cfg.Targets {
		sn.targets[i] = t.Name
	}
	return sn
}

type snapshotKey struct{}

// snapshotFrom returns the snapshot of the tools/call or tools/list request in ctx.
func snapshotFrom(ctx context.Context) *snapshot {
	sn, _ := ctx.Value(snapshotKey{}).(*snapshot)
	return sn
}

// replace makes next the current snapshot and closes the clients of the one it replaces once the tool calls holding
// it end.
func (s *toolServer) replace(next *snapshot) {
	s.mu.Lock()
	prev := s.current
	s.current = next
	s.mu.Unlock()
	go func() {
		prev.holders.Wait()
		prev.clients.Close()
	}()
}

// pin gives each tools/call request the current snapshot, which it holds until it ends, and each tools/list request
// the current snapshot.
func pin(s *toolServer) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch req.(type) {
			case *mcp.CallToolRequest:
				s.mu.Lock()
				sn := s.current
				sn.holders.Add(1)
				s.mu.Unlock()
				defer sn.holders.Done()
				ctx = context.WithValue(ctx, snapshotKey{}, sn)
			case *mcp.ListToolsRequest:
				s.mu.Lock()
				ctx = context.WithValue(ctx, snapshotKey{}, s.current)
				s.mu.Unlock()
			}
			return next(ctx, method, req)
		}
	}
}

// pinned returns a handler that calls the handler h returns for the snapshot of each call.
func pinned[In, Out any](h func(*snapshot) mcp.ToolHandlerFor[In, Out]) mcp.ToolHandlerFor[In, Out] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		return h(snapshotFrom(ctx))(ctx, req, in)
	}
}
