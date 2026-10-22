// Middleware for MCP requests: one turns a panic in any request into an internal error, one
// cancels every request once the server shuts down, and one drops null tool arguments and turns
// every tool error, including a handler panic, into a toolError, rewriting the message for
// arguments that break a tool's input schema in plain words.

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
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

var errServerShutdown = errors.New("the server is shutting down")

// cancelWith cancels subscription listeners when subscriptions is done, and
// every request when stop is done, whatever its MCP protocol version.
func cancelWith(stop, subscriptions context.Context) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(nil)
			shutdown := func() { cancel(errServerShutdown) }
			defer context.AfterFunc(stop, shutdown)()
			if method == "subscriptions/listen" {
				defer context.AfterFunc(subscriptions, shutdown)()
			}
			return next(ctx, method, req)
		}
	}
}

// normalizeToolCalls drops null-valued arguments and makes every tool error a toolError, including a handler panic,
// rewriting the message for arguments that break a tool's input schema, from inputs, with argumentError; for arguments
// that are not a JSON object it calls next first so an unknown tool still gets the SDK's error.
func normalizeToolCalls(inputs map[string]*jsonschema.Schema) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
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
			var te *toolError
			cause := r.GetError()
			if errors.As(cause, &te) {
				cause = te.cause
			}
			// Only cancellation caused by shutdown is retryable. A successful call,
			// client cancellation or unrelated failure keeps its original result.
			if errors.Is(context.Cause(ctx), errServerShutdown) && serviceerror.ToStatus(cause).Code() == codes.Canceled {
				return errorResult(&toolError{Code: "server_shutting_down", Message: errServerShutdown.Error(), Retryable: true}), nil
			}
			if te != nil {
				return errorResult(te), nil
			}
			if msg, found := strings.CutPrefix(fmt.Sprint(r.GetError()), `validating "arguments": `); found {
				return errorResult(&toolError{Code: "invalid_argument", Message: argumentError(msg, inputs[call.Params.Name], call.Params.Arguments)}), nil
			}
			return errorResult(&toolError{Code: "internal", Message: "internal error", cause: r.GetError()}), nil
		}
	}
}

// argumentError rewrites msg, the validator's message for arguments args that break schema, such as
//
//	validating root: validating /properties/task_queues: validating /properties/task_queues/items: minLength: "" contains 0 Unicode code points, fewer than 1
//
// to name each argument at fault by its path in args and say what is wrong with it, such as
// "task_queues[0] must not be empty". It returns msg unchanged if it does not know the rule.
func argumentError(msg string, schema *jsonschema.Schema, args json.RawMessage) string {
	// The validator prefixes the rule's message with the path of each schema it descends into, so the last
	// prefix is the path of the schema whose rule failed.
	rule, schemaPath := msg, ""
	for {
		rest, ok := strings.CutPrefix(rule, "validating ")
		if !ok {
			break
		}
		if schemaPath, rule, ok = strings.Cut(rest, ": "); !ok {
			return msg
		}
	}
	var arg any = map[string]any{}
	segs := strings.Split(strings.TrimPrefix(schemaPath, "root"), "/")
	if schema == nil || segs[0] != "" || len(args) > 0 && json.Unmarshal(args, &arg) != nil {
		return msg
	}
	// Follow the schema path, such as /properties/workflows/items/properties/workflow_id, through schema and args.
	path := ""
	field := func(name string) string { return strings.TrimPrefix(path+"."+name, ".") }
	for segs = segs[1:]; len(segs) > 0; {
		switch {
		case segs[0] == "properties" && len(segs) > 1:
			obj, _ := arg.(map[string]any)
			path, schema, arg = field(segs[1]), schema.Properties[segs[1]], obj[segs[1]]
			segs = segs[2:]
		case segs[0] == "items" && schema.Items != nil:
			// The validator stops at the first item that breaks the items schema, so that is the item at fault.
			items, _ := arg.([]any)
			r, err := schema.Items.Resolve(nil)
			if err != nil {
				return msg
			}
			i := slices.IndexFunc(items, func(item any) bool { return r.Validate(item) != nil })
			if i < 0 {
				return msg
			}
			path, schema, arg = fmt.Sprintf("%s[%d]", path, i), schema.Items, items[i]
			segs = segs[1:]
		default:
			return msg
		}
		if schema == nil {
			return msg
		}
	}
	obj, _ := arg.(map[string]any)
	var problems []string
	keyword, _, _ := strings.Cut(rule, ":")
	switch {
	case keyword == "required":
		for _, name := range schema.Required {
			if _, ok := obj[name]; !ok {
				problems = append(problems, field(name)+" is required")
			}
		}
	case strings.HasPrefix(rule, "unexpected additional properties "):
		for _, name := range slices.Sorted(maps.Keys(obj)) {
			if _, ok := schema.Properties[name]; !ok {
				problems = append(problems, field(name)+" is not a known argument")
			}
		}
	case path == "":
		// Every other rule is on an argument, not on the arguments as a whole.
	case keyword == "type":
		var types []string
		for _, t := range append([]string{schema.Type}, schema.Types...) {
			if t == "" || t == "null" {
				continue
			}
			article := "a "
			if strings.ContainsRune("aeiou", rune(t[0])) {
				article = "an "
			}
			types = append(types, article+t)
		}
		if len(types) > 0 {
			problems = []string{path + " must be " + strings.Join(types, " or ")}
		}
	case keyword == "enum":
		values := make([]string, len(schema.Enum))
		for i, v := range schema.Enum {
			values[i] = fmt.Sprint(v)
		}
		problems = []string{path + " must be one of " + strings.Join(values, ", ")}
	case keyword == "minLength" && *schema.MinLength == 1, keyword == "minItems" && *schema.MinItems == 1:
		problems = []string{path + " must not be empty"}
	case keyword == "maxItems":
		problems = []string{fmt.Sprintf("%s must have at most %d items", path, *schema.MaxItems)}
	case keyword == "minimum":
		problems = []string{path + " must be at least " + strconv.FormatFloat(*schema.Minimum, 'f', -1, 64)}
	case keyword == "maximum":
		problems = []string{path + " must be at most " + strconv.FormatFloat(*schema.Maximum, 'f', -1, 64)}
	}
	if len(problems) == 0 {
		return msg
	}
	return strings.Join(problems, "; ")
}

func errorResult(err error) *mcp.CallToolResult {
	var res mcp.CallToolResult
	res.SetError(err)
	return &res
}
