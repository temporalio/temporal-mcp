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
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func logToolCalls(logger *slog.Logger) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			call, ok := req.(*mcp.CallToolRequest)
			if !ok {
				return next(ctx, method, req)
			}
			var target string
			var fields map[string]json.RawMessage
			if json.Unmarshal(call.Params.Arguments, &fields) == nil {
				_ = json.Unmarshal(fields["target"], &target)
			}
			start := time.Now()
			res, err := next(ctx, method, req)
			level, code := slog.LevelInfo, ""
			var cause error
			var te *toolError
			switch r, _ := res.(*mcp.CallToolResult); {
			case err != nil:
				level = slog.LevelWarn
			case r.IsError && errors.As(r.GetError(), &te):
				code, cause = te.Code, te.cause
				if code == "internal" {
					level = slog.LevelError
				}
			}
			attrs := []slog.Attr{
				slog.String("tool", call.Params.Name),
				slog.String("target", target),
				slog.Duration("duration", time.Since(start)),
				slog.String("error_code", code),
			}
			if cause != nil {
				attrs = append(attrs, slog.String("error", cause.Error()))
			}
			logger.LogAttrs(ctx, level, "tool call", attrs...)
			return res, err
		}
	}
}

// normalizeToolCalls drops null-valued arguments and makes every tool error a toolError, including a handler panic, which would otherwise end the process; for arguments that are not a JSON object it calls next first so an unknown tool still gets the SDK's error.
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
