// Package temporal provides one Temporal client per target, plus a Cloud Ops API
// client for each Temporal Cloud target with an API key, and limits the calls each
// tool can make through them. Each call's API key comes from a credential that sees
// the caller. It also lists the namespaces and user groups of a Temporal Cloud account.
package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identitypb "go.temporal.io/cloud-sdk/api/identity/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/identity"
)

// Clients holds a Temporal client for each target.
type Clients struct {
	byName map[string]client.Client
	cloud  map[string]cloudservice.CloudServiceClient
}

// An Option changes how NewClients, ListNamespaces and ListUserGroups connect. Options are for tests.
type Option func(*options)

type options struct {
	rootCAs         *x509.CertPool
	cloudOpsAddress string
}

// WithRootCAs makes the clients trust only the CAs in pool, rather than the system's, when they connect to targets with a client certificate.
func WithRootCAs(pool *x509.CertPool) Option {
	return func(o *options) { o.rootCAs = pool }
}

// WithPlaintextCloudOpsAPI makes the clients call the Temporal Cloud Ops API at address, in plaintext, rather than at its public endpoint.
func WithPlaintextCloudOpsAPI(address string) Option {
	return func(o *options) { o.cloudOpsAddress = address }
}

// NewClients returns the clients for targets. Calls identify this server by
// name and version in the gRPC user agent and the client-name and
// client-version headers. The clients log to logger. It fails if a client
// certificate cannot be loaded, if a target's address cannot be parsed, or if a
// Cloud Ops API client cannot be created.
func NewClients(targets []config.Target, name, version string, logger *slog.Logger, opts ...Option) (*Clients, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	c := &Clients{byName: map[string]client.Client{}, cloud: map[string]cloudservice.CloudServiceClient{}}
	var errs []error
	for i, t := range targets {
		cred := apiKeyCredential(t.APIKey, logger)
		if t.TLS != nil {
			if err := t.TLS.Check(logger); err != nil {
				errs = append(errs, fmt.Errorf("targets[%d]: tls: %w", i, err))
				continue
			}
		}
		cl, err := lazyClient(t, name, version, cred, o.rootCAs, logger)
		if err != nil {
			errs = append(errs, fmt.Errorf("targets[%d]: address: %w", i, err))
			continue
		}
		c.byName[t.Name] = cl
		if cred != nil && config.IsCloud(t.Address) {
			ops, err := cloudOpsClient(apiKeyReader(func(ctx context.Context) (string, error) {
				return cred(ctx, identity.CallerFrom(ctx), &t)
			}), name, version, o.cloudOpsAddress, grpc.WithUnaryInterceptor(restrictToTool(t.Name, t.Namespace)))
			if err != nil {
				errs = append(errs, fmt.Errorf("targets[%d]: %w", i, err))
				continue
			}
			c.cloud[t.Name] = ops.CloudService()
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return c, nil
}

// A credential returns the API key for each Temporal call to target; caller is nil without auth.
type credential func(ctx context.Context, caller *identity.Caller, target *config.Target) (string, error)

func apiKeyCredential(k *config.Secret, logger *slog.Logger) credential {
	if k == nil {
		return nil
	}
	return func(ctx context.Context, _ *identity.Caller, t *config.Target) (string, error) {
		key, err := k.Read()
		if err != nil {
			logger.ErrorContext(ctx, "cannot read API key", "target", t.Name, "error", err.Error())
			return "", errors.New("this server's API key for the target could not be read; the cause is in this server's log")
		}
		return key, nil
	}
}

func lazyClient(t config.Target, name, version string, cred credential, rootCAs *x509.CertPool, logger *slog.Logger) (client.Client, error) {
	opts := client.Options{
		HostPort:  t.Address,
		Namespace: t.Namespace,
		Logger:    tlog.NewStructuredLogger(logger),
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(restrictToTool(t.Name, t.Namespace)),
				grpc.WithChainUnaryInterceptor(identify(name, version)),
				grpc.WithUserAgent(name + "/" + version),
			},
		},
	}
	if t.TLS != nil {
		opts.ConnectionOptions.TLS = &tls.Config{RootCAs: rootCAs, GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := t.TLS.Load()
			if err != nil {
				logger.Error("cannot load client certificate", "target", t.Name, "error", err.Error())
			}
			return cert, err
		}}
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

// CloudOps returns the Temporal Cloud Ops API client for the target with the given name, or nil if the target is not on Temporal Cloud or has no API key.
func (c *Clients) CloudOps(name string) cloudservice.CloudServiceClient {
	return c.cloud[name]
}

// CallTimeout bounds a tool's call to a Temporal server or the Cloud Ops API, including its retries while the server is unavailable, and each page that ListNamespaces and ListUserGroups read.
const CallTimeout = 10 * time.Second

// ListNamespaces returns every namespace that the Temporal Cloud Ops API lists
// for apiKey, reading all its pages, and identifies this server by name and
// version as NewClients' clients do. Each page's call fails if it takes longer
// than CallTimeout.
func ListNamespaces(ctx context.Context, apiKey, name, version string, opts ...Option) ([]*namespacepb.Namespace, error) {
	return listPages(ctx, apiKey, name, version, opts, func(ctx context.Context, c cloudservice.CloudServiceClient, token string) ([]*namespacepb.Namespace, string, error) {
		resp, err := c.GetNamespaces(ctx, &cloudservice.GetNamespacesRequest{PageToken: token})
		return resp.GetNamespaces(), resp.GetNextPageToken(), err
	})
}

// ListUserGroups returns every user group that the Temporal Cloud Ops API lists
// for apiKey, as ListNamespaces returns namespaces.
func ListUserGroups(ctx context.Context, apiKey, name, version string, opts ...Option) ([]*identitypb.UserGroup, error) {
	return listPages(ctx, apiKey, name, version, opts, func(ctx context.Context, c cloudservice.CloudServiceClient, token string) ([]*identitypb.UserGroup, string, error) {
		resp, err := c.GetUserGroups(ctx, &cloudservice.GetUserGroupsRequest{PageToken: token})
		return resp.GetGroups(), resp.GetNextPageToken(), err
	})
}

// listPages returns the items of every page that page reads, following next page tokens, each call bounded by CallTimeout.
func listPages[T any](ctx context.Context, apiKey, name, version string, opts []Option, page func(context.Context, cloudservice.CloudServiceClient, string) ([]T, string, error)) ([]T, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	c, err := cloudOpsClient(apiKeyReader(func(context.Context) (string, error) { return apiKey, nil }), name, version, o.cloudOpsAddress)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var items []T
	var token string
	for {
		pageCtx, cancel := context.WithTimeout(ctx, CallTimeout)
		got, next, err := page(pageCtx, c.CloudService(), token)
		cancel()
		if err != nil {
			return nil, err
		}
		items = append(items, got...)
		if token = next; token == "" {
			return items, nil
		}
	}
}

// cloudOpsClient returns a client for the Cloud Ops API that calls it with the API key from key and the same identity
// as the Temporal clients, dialing with the options in opts.
func cloudOpsClient(key apiKeyReader, name, version, cloudOpsAddress string, opts ...grpc.DialOption) (*cloudclient.Client, error) {
	o := cloudclient.Options{
		APIKeyReader: key,
		UserAgent:    name + "/" + version,
		// Its retries would hide a rate limit from the agent; the tools retry only an unavailable API themselves.
		DisableRetry:    true,
		GRPCDialOptions: append(opts, grpc.WithChainUnaryInterceptor(identify(name, version))),
	}
	if cloudOpsAddress != "" {
		o.HostPort, o.AllowInsecure = cloudOpsAddress, true
	}
	c, err := cloudclient.New(o)
	if err != nil {
		return nil, fmt.Errorf("creating the Cloud Ops API client: %w", err)
	}
	return c, nil
}

type apiKeyReader func(context.Context) (string, error)

func (r apiKeyReader) GetAPIKey(ctx context.Context) (string, error) {
	return r(ctx)
}

type toolKey struct{}

type tool struct {
	name    string
	rpcs    []string
	targets []string
}

// WithTool returns a context for Temporal calls made by the named tool, which
// may call only the given Temporal RPCs (full gRPC method names) and
// only on the targets with the given names. A request with a namespace field
// must set it to the target's configured namespace; an empty one is rejected.
// RPCs whose requests lack the field, such as ListNamespaces and the Nexus
// endpoint RPCs, are limited only by the RPC list.
// A Temporal call whose context lacks a tool is rejected.
func WithTool(ctx context.Context, name string, rpcs, targets []string) context.Context {
	return context.WithValue(ctx, toolKey{}, tool{name, rpcs, targets})
}

type callsKey struct{}

// CountCalls returns a context whose Temporal calls are counted in n: those that
// pass the limits set by WithTool, with every attempt of a retried call counted.
func CountCalls(ctx context.Context, n *atomic.Int64) context.Context {
	return context.WithValue(ctx, callsKey{}, n)
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

func restrictToTool(target, namespace string) grpc.UnaryClientInterceptor {
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
		if r, ok := req.(interface{ GetNamespace() string }); ok && r.GetNamespace() != namespace {
			return status.Errorf(codes.PermissionDenied, "tool %s is not allowed to call namespace %q through target %s", t.name, r.GetNamespace(), target)
		}
		if n, ok := ctx.Value(callsKey{}).(*atomic.Int64); ok {
			n.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
