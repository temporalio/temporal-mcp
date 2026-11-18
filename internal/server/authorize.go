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

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/identity"
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
// A profile that gives start_workflow also gives list_startable_workflows.
func grants(a *config.Authorization, req mcp.Request) map[string][]string {
	g := map[string][]string{}
	for _, p := range callerProfiles(a, req) {
		for _, tool := range p.Tools {
			g[tool] = append(g[tool], p.Targets...)
			if tool == "start_workflow" {
				g["list_startable_workflows"] = append(g["list_startable_workflows"], p.Targets...)
			}
		}
	}
	return g
}

// callerProfiles returns the profile of each rule that the token of req matches.
func callerProfiles(a *config.Authorization, req mcp.Request) []config.Profile {
	extra := req.GetExtra()
	if extra == nil || extra.TokenInfo == nil {
		return nil
	}
	caller := identity.CallerFromTokenInfo(extra.TokenInfo)
	var profiles []config.Profile
	for _, r := range a.Rules {
		if matches(r, caller) {
			profiles = append(profiles, a.Profiles[r.Profile])
		}
	}
	return profiles
}

// startableWorkflows returns the workflows of t that the caller of req may start: without auth, every one, and with
// it, each one whose type is in the workflows of a caller's profile that has t in its targets.
func startableWorkflows(cfg *config.Config, req mcp.Request, t *config.Target) []config.Workflow {
	if cfg.Auth == nil {
		return t.Workflows
	}
	var types []string
	for _, p := range callerProfiles(cfg.Authorization, req) {
		if slices.Contains(p.Targets, t.Name) {
			types = append(types, p.Workflows...)
		}
	}
	return slices.DeleteFunc(slices.Clone(t.Workflows), func(w config.Workflow) bool { return !slices.Contains(types, w.Type) })
}

func matches(r config.Rule, caller *identity.Caller) bool {
	if r.ClaimName == "scope" || r.ClaimName == "scp" {
		return slices.Contains(caller.Scopes(r.ClaimName), r.ClaimValue)
	}
	switch v := caller.Claims[r.ClaimName].(type) {
	case string:
		return v == r.ClaimValue
	case []any:
		return slices.Contains(v, any(r.ClaimValue))
	}
	return false
}

func resolveProfileTools(a *config.Authorization, tools []string, access map[string]string) error {
	if a == nil {
		return nil
	}
	tools = slices.DeleteFunc(slices.Clone(tools), func(t string) bool { return t == "list_startable_workflows" })
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(a.Profiles)) {
		p := a.Profiles[name]
		for _, f := range []struct {
			key   string
			tools []string
		}{{"tools", p.Tools}, {"tools_deny", p.ToolsDeny}} {
			for k, tool := range f.tools {
				switch {
				case tool == "list_startable_workflows":
					errs = append(errs, fmt.Errorf("authorization: profiles: %q: %s[%d]: list_startable_workflows comes with start_workflow, so profiles do not list it", name, f.key, k))
				case !slices.Contains(tools, tool):
					errs = append(errs, fmt.Errorf("authorization: profiles: %q: %s[%d]: unknown tool %q; the tools are %s", name, f.key, k, tool, strings.Join(tools, ", ")))
				}
			}
		}
		if p.Tools == nil {
			p.Tools = slices.DeleteFunc(slices.Clone(tools), func(t string) bool { return access[t] != "read" || slices.Contains(p.ToolsDeny, t) })
			if len(p.Tools) == 0 {
				errs = append(errs, fmt.Errorf("authorization: profiles: %q: tools_deny: leaves the profile no tools", name))
			}
			a.Profiles[name] = p
		}
		switch start := slices.Contains(p.Tools, "start_workflow"); {
		case start && len(p.Workflows) == 0:
			errs = append(errs, fmt.Errorf("authorization: profiles: %q: workflows: required with start_workflow in tools", name))
		case !start && len(p.Workflows) > 0:
			errs = append(errs, fmt.Errorf("authorization: profiles: %q: workflows: not allowed without start_workflow in tools", name))
		}
	}
	return errors.Join(errs...)
}
