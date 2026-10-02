// Package temporal provides one Temporal client per target and limits the
// calls each tool can make through it.
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
		opts := client.Options{
			HostPort:  t.Address,
			Namespace: t.Namespace,
			ConnectionOptions: client.ConnectionOptions{
				DialOptions: []grpc.DialOption{
					grpc.WithUnaryInterceptor(allowDeclared),
					grpc.WithChainUnaryInterceptor(identify(name, version)),
					grpc.WithUserAgent(name + "/" + version),
				},
			},
		}
		if k := t.APIKey; k != nil {
			if k.Env != "" {
				key := strings.TrimSpace(os.Getenv(k.Env))
				if key == "" {
					errs = append(errs, fmt.Errorf("targets[%d]: api_key: environment variable %s is unset or empty", i, k.Env))
					continue
				}
				opts.Credentials = client.NewAPIKeyStaticCredentials(key)
			} else {
				opts.Credentials = client.NewAPIKeyDynamicCredentials(func(context.Context) (string, error) {
					return readAPIKey(k.File)
				})
			}
		}
		cl, err := client.NewLazyClient(opts)
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
	name string
	rpcs []string
}

// WithTool returns a context for Temporal calls made by the named tool, which
// may call only the given WorkflowService RPCs (full gRPC method names).
// A Temporal call whose context lacks a tool is rejected.
func WithTool(ctx context.Context, name string, rpcs []string) context.Context {
	return context.WithValue(ctx, toolKey{}, tool{name, rpcs})
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

func allowDeclared(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	t, ok := ctx.Value(toolKey{}).(tool)
	if !ok {
		return status.Errorf(codes.PermissionDenied, "%s was called outside a tool", method)
	}
	if !slices.Contains(t.rpcs, method) {
		return status.Errorf(codes.PermissionDenied, "tool %s is not allowed to call %s", t.name, method)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}
