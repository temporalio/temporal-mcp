// The audit record: one line of JSON for each tools/call request, written by the audit
// middleware once the call is done.

package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/identity"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

type auditRecord struct {
	Time          time.Time       `json:"time"`
	Issuer        *string         `json:"issuer"`
	Subject       *string         `json:"subject"`
	Tool          string          `json:"tool"`
	Access        *string         `json:"access"`
	Target        *string         `json:"target"`
	Arguments     json.RawMessage `json:"arguments"`
	Outcome       string          `json:"outcome"`
	DurationMS    int64           `json:"duration_ms"`
	TemporalCalls int64           `json:"temporal_calls"`
	denied        bool
	calls         atomic.Int64
}

type auditKey struct{}

// auditRecordFrom returns the audit record of the tool call in ctx, or nil outside one.
func auditRecordFrom(ctx context.Context) *auditRecord {
	rec, _ := ctx.Value(auditKey{}).(*auditRecord)
	return rec
}

// audit writes a record of each tools/call request to w. A call whose record cannot be
// written fails with an internal error. access gives each tool's access, read or write.
func audit(w io.Writer, logger *slog.Logger, cfg *config.Config, access map[string]string) mcp.Middleware {
	var mu sync.Mutex
	var dirty bool
	write := func(line []byte) error {
		mu.Lock()
		defer mu.Unlock()
		// After a partial write, a newline starts the record on a line of its own.
		if dirty {
			line = append([]byte{'\n'}, line...)
		}
		n, err := w.Write(line)
		dirty = err != nil && (dirty || n > 0)
		return err
	}
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			call, ok := req.(*mcp.CallToolRequest)
			if !ok {
				return next(ctx, method, req)
			}
			start := time.Now()
			rec := &auditRecord{Time: start.UTC(), Tool: call.Params.Name, Arguments: call.Params.Arguments}
			if a, ok := access[call.Params.Name]; ok {
				rec.Access = &a
			}
			if call.Extra != nil && call.Extra.TokenInfo != nil {
				caller := identity.CallerFromTokenInfo(call.Extra.TokenInfo)
				rec.Issuer, rec.Subject = &caller.Issuer, &caller.Subject
			}
			rec.Target = callTarget(cfg, call)
			res, err := next(temporal.CountCalls(context.WithValue(ctx, auditKey{}, rec), &rec.calls), method, req)
			rec.DurationMS, rec.TemporalCalls = time.Since(start).Milliseconds(), rec.calls.Load()
			var cause error
			var te *toolError
			switch r, _ := res.(*mcp.CallToolResult); {
			case err != nil && rec.Access == nil:
				rec.Outcome = "unknown_tool"
			case rec.denied:
				rec.Outcome = "denied"
			case err != nil:
				rec.Outcome, cause = "internal", err
			case r.IsError && errors.As(r.GetError(), &te) && te.Code == "internal":
				rec.Outcome, cause = te.Code, te.cause
			case r.IsError && errors.As(r.GetError(), &te):
				rec.Outcome = te.Code
			default:
				rec.Outcome = "ok"
			}
			// The client cancelled or disconnected, so the error came from the cancellation.
			if ctx.Err() != nil && (te != nil && te.Code != "internal" || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)) {
				rec.Outcome, cause = "cancelled", nil
			}
			if cause != nil {
				logger.ErrorContext(ctx, "tool call failed", "tool", rec.Tool, "error", cause.Error())
			}
			b, werr := json.Marshal(rec)
			if werr == nil {
				werr = write(append(b, '\n'))
			}
			if werr != nil {
				logger.ErrorContext(ctx, "cannot write audit record", "tool", rec.Tool, "error", werr.Error())
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "internal error"}
			}
			return res, err
		}
	}
}

// callTarget returns the target that call's arguments name, resolving an alias to the target's name when cfg has
// it, or nil if they name none.
func callTarget(cfg *config.Config, call *mcp.CallToolRequest) *string {
	var fields map[string]json.RawMessage
	var target *string
	if json.Unmarshal(call.Params.Arguments, &fields) != nil || json.Unmarshal(fields["target"], &target) != nil || target == nil {
		return nil
	}
	if t, ok := cfg.Lookup(*target); ok {
		return &t.Name
	}
	return target
}
