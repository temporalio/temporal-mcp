// Authorization: which tools each caller sees and may call, and on which targets it may
// use each one.

package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
)

type targetsKey struct{}

// authorize leaves out of tools/list the tools the caller may use on no target,
// answers a call to one as the SDK answers a call to an unknown tool, and gives
// every other tool call the targets its caller may use the tool on, or, for
// list_available_targets, the targets its caller may use any tool on. Without
// auth, every caller may use every tool on every target.
func authorize(cfg *config.Config) mcp.Middleware {
	all := make([]string, len(cfg.Targets))
	for i, t := range cfg.Targets {
		all[i] = t.Name
	}
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch r := req.(type) {
			case *mcp.ListToolsRequest:
				res, err := next(ctx, method, req)
				if cfg.Auth == nil || err != nil {
					return res, err
				}
				list, ok := res.(*mcp.ListToolsResult)
				if !ok || list == nil {
					return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "internal error"}
				}
				g := grants(cfg.Authorization, req)
				list.Tools = slices.DeleteFunc(list.Tools, func(t *mcp.Tool) bool { return t == nil || len(g[t.Name]) == 0 })
				list.CacheScope = "private"
				return list, err
			case *mcp.CallToolRequest:
				targets := all
				if cfg.Auth != nil {
					g := grants(cfg.Authorization, req)
					if targets = g[r.Params.Name]; len(targets) == 0 {
						if rec := auditRecordFrom(ctx); rec != nil {
							rec.denied = true
						}
						return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: fmt.Sprintf("unknown tool %q", r.Params.Name)}
					}
					if r.Params.Name == "list_available_targets" {
						targets = slices.Concat(slices.Collect(maps.Values(g))...)
					}
				}
				ctx = context.WithValue(ctx, targetsKey{}, targets)
			}
			return next(ctx, method, req)
		}
	}
}

// toolTargets returns the names of the targets the tool call in ctx may use.
func toolTargets(ctx context.Context) []string {
	targets, _ := ctx.Value(targetsKey{}).([]string)
	return targets
}

// mayUse reports whether the caller of req may use tool on the target with the given name.
func mayUse(cfg *config.Config, req mcp.Request, tool, target string) bool {
	return cfg.Auth == nil || slices.Contains(grants(cfg.Authorization, req)[tool], target)
}

// grants returns, for each tool, the targets the caller of req may use it on:
// those of every profile that gives it the tool through a rule its token matches.
func grants(a *config.Authorization, req mcp.Request) map[string][]string {
	g := map[string][]string{}
	extra := req.GetExtra()
	if extra == nil || extra.TokenInfo == nil {
		return g
	}
	caller := identity.CallerFromTokenInfo(extra.TokenInfo)
	for _, r := range a.Rules {
		if !matches(r, caller) {
			continue
		}
		p := a.Profiles[r.Profile]
		for _, tool := range p.Tools {
			g[tool] = append(g[tool], p.Targets...)
		}
	}
	return g
}

func matches(r config.Rule, caller *identity.Caller) bool {
	if r.Issuer != "" && r.Issuer != caller.Issuer {
		return false
	}
	switch v := caller.Claims[r.ClaimName].(type) {
	case string:
		if r.ClaimName == "scope" || r.ClaimName == "scp" {
			return slices.Contains(strings.Split(v, " "), r.ClaimValue)
		}
		return v == r.ClaimValue
	case []any:
		return slices.Contains(v, any(r.ClaimValue))
	}
	return false
}

func checkProfileTools(a *config.Authorization, tools []string) error {
	if a == nil {
		return nil
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(a.Profiles)) {
		for k, tool := range a.Profiles[name].Tools {
			if !slices.Contains(tools, tool) {
				errs = append(errs, fmt.Errorf("authorization: profiles: %q: tools[%d]: unknown tool %q; the tools are %s", name, k, tool, strings.Join(tools, ", ")))
			}
		}
	}
	return errors.Join(errs...)
}
