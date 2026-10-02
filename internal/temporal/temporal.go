// Package temporal provides one Temporal client per target and limits the calls each tool
// can make through it. Each call's API key comes from a credential that sees the caller.
package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
)

// Clients holds a Temporal client for each target.
type Clients struct {
	byName map[string]client.Client
}

// NewClients returns the clients for targets. Calls identify this server by
// name and version in the gRPC user agent and the client-name and
// client-version headers. It fails if an API key's environment variable is
// unset or empty, or if a target's address cannot be parsed.
func NewClients(targets []config.Target, name, version string) (*Clients, error) {
	c := &Clients{byName: map[string]client.Client{}}
	var errs []error
	for i, t := range targets {
		cred, err := apiKeyCredential(t.APIKey)
		if err != nil {
			errs = append(errs, fmt.Errorf("targets[%d]: api_key: %w", i, err))
			continue
		}
		cl, err := lazyClient(t, name, version, cred)
		if err != nil {
			errs = append(errs, fmt.Errorf("targets[%d]: address: %w", i, err))
			continue
		}
		c.byName[t.Name] = cl
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return c, nil
}

// A credential returns the API key for each Temporal call to target; caller is nil without auth.
type credential func(ctx context.Context, caller *identity.Caller, target *config.Target) (string, error)

func apiKeyCredential(k *config.APIKey) (credential, error) {
	switch {
	case k == nil:
		return nil, nil
	case k.Env != "":
		key := strings.TrimSpace(os.Getenv(k.Env))
		if key == "" {
			return nil, fmt.Errorf("environment variable %s is unset or empty", k.Env)
		}
		return func(context.Context, *identity.Caller, *config.Target) (string, error) { return key, nil }, nil
	default:
		return func(context.Context, *identity.Caller, *config.Target) (string, error) { return readAPIKey(k.File) }, nil
	}
}

func lazyClient(t config.Target, name, version string, cred credential) (client.Client, error) {
	opts := client.Options{
		HostPort:  t.Address,
		Namespace: t.Namespace,
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(restrictToTool(t.Name)),
				grpc.WithChainUnaryInterceptor(identify(name, version)),
				grpc.WithUserAgent(name + "/" + version),
			},
		},
	}
	if cred != nil {
		opts.Credentials = client.NewAPIKeyDynamicCredentials(func(ctx context.Context) (string, error) {
			return cred(ctx, identity.CallerFrom(ctx), &t)
		})
	}
	return client.NewLazyClient(opts)
}

// Client returns the client for the target with the given name.
func (c *Clients) Client(name string) client.Client {
	return c.byName[name]
}

func readAPIKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read API key: %w", err)
	}
	key := strings.TrimSpace(string(data))
	if key == "" {
		return "", fmt.Errorf("read API key: %s is empty", path)
	}
	return key, nil
}

type toolKey struct{}

type tool struct {
	name    string
	rpcs    []string
	targets []string
}

// WithTool returns a context for Temporal calls made by the named tool, which
// may call only the given WorkflowService RPCs (full gRPC method names) and
// only on the targets with the given names.
// A Temporal call whose context lacks a tool is rejected.
func WithTool(ctx context.Context, name string, rpcs, targets []string) context.Context {
	return context.WithValue(ctx, toolKey{}, tool{name, rpcs, targets})
}

func identify(name, version string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		if md == nil {
			md = metadata.MD{}
		}
		md.Set("client-name", name)
		md.Set("client-version", version)
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	}
}

func restrictToTool(target string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		t, ok := ctx.Value(toolKey{}).(tool)
		if !ok {
			return status.Errorf(codes.PermissionDenied, "%s was called outside a tool", method)
		}
		if !slices.Contains(t.rpcs, method) {
			return status.Errorf(codes.PermissionDenied, "tool %s is not allowed to call %s", t.name, method)
		}
		if !slices.Contains(t.targets, target) {
			return status.Errorf(codes.PermissionDenied, "tool %s is not allowed to call target %s", t.name, target)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
