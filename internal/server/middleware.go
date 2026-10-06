// Middleware for MCP requests: one turns a panic in any request into an internal error, and
// one drops null tool arguments and turns every tool error, including a handler panic, into a
// toolError.

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// recoverPanics answers a request that panics with an internal error: the SDK does not recover, so the panic would end the process.
func recoverPanics(logger *slog.Logger) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.ErrorContext(ctx, "request panicked", "method", method, "error", fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
					res, err = nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "internal error"}
				}
			}()
			return next(ctx, method, req)
		}
	}
}

// normalizeToolCalls drops null-valued arguments and makes every tool error a toolError, including a handler panic; for arguments that are not a JSON object it calls next first so an unknown tool still gets the SDK's error.
func normalizeToolCalls(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
		call, ok := req.(*mcp.CallToolRequest)
		if !ok {
			return next(ctx, method, req)
		}
		defer func() {
			if r := recover(); r != nil {
				res, err = errorResult(&toolError{Code: "internal", Message: "internal error", cause: fmt.Errorf("panic: %v\n%s", r, debug.Stack())}), nil
			}
		}()
		if len(call.Params.Arguments) > 0 {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(call.Params.Arguments, &fields); err != nil {
				if res, err := next(ctx, method, req); err != nil {
					return res, err
				}
				return errorResult(&toolError{Code: "invalid_argument", Message: "arguments must be a JSON object"}), nil
			}
			maps.DeleteFunc(fields, func(_ string, v json.RawMessage) bool { return string(v) == "null" })
			call.Params.Arguments, _ = json.Marshal(fields)
		}
		res, err = next(ctx, method, req)
		r, ok := res.(*mcp.CallToolResult)
		if !ok || err != nil || !r.IsError {
			return res, err
		}
		if te := (*toolError)(nil); errors.As(r.GetError(), &te) {
			return errorResult(te), nil
		}
		if msg, found := strings.CutPrefix(fmt.Sprint(r.GetError()), `validating "arguments": `); found {
			return errorResult(&toolError{Code: "invalid_argument", Message: msg}), nil
		}
		return errorResult(&toolError{Code: "internal", Message: "internal error", cause: r.GetError()}), nil
	}
}

func errorResult(err error) *mcp.CallToolResult {
	var res mcp.CallToolResult
	res.SetError(err)
	return &res
}
