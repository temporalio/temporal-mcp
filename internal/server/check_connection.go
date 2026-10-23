// The check_connection tool, which checks that a target's Temporal server and namespace,
// and its codec server if it has one, are reachable, and the wording of failed Temporal
// calls, which hides server addresses.

package server

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

const stageTimeout = 5 * time.Second

type checkConnectionInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target"`
}

type connectionCheck struct {
	Stages []stage `json:"stages"`
}

type stage struct {
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	LatencyMS *int64  `json:"latency_ms"`
	Reason    *string `json:"reason"`
}

func checkConnection(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) mcp.ToolHandlerFor[checkConnectionInput, envelope[connectionCheck]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in checkConnectionInput) (*mcp.CallToolResult, envelope[connectionCheck], error) {
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, envelope[connectionCheck]{}, err
		}
		svc := clients.Client(t.Name).WorkflowService()
		server, err := runStage(ctx, "server", temporalCheck(t, func(ctx context.Context, opts ...grpc.CallOption) error {
			_, err := svc.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, opts...)
			return err
		}))
		if err != nil {
			return nil, envelope[connectionCheck]{}, err
		}
		namespace := stage{Name: "namespace", Status: "skipped", Reason: new("not run because the server stage failed")}
		if server.Status == "ok" {
			namespace, err = runStage(ctx, "namespace", temporalCheck(t, func(ctx context.Context, opts ...grpc.CallOption) error {
				_, err := svc.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: t.Namespace}, opts...)
				return err
			}))
			if err != nil {
				return nil, envelope[connectionCheck]{}, err
			}
		}
		codec := stage{Name: "codec", Status: "skipped", Reason: new("not run because the target does not decode payloads")}
		if t.PayloadHandling == config.PayloadDecode {
			codec, err = runStage(ctx, "codec", func(ctx context.Context) string { return payloads.CheckCodec(ctx, t.Name) })
			if err != nil {
				return nil, envelope[connectionCheck]{}, err
			}
		}
		return nil, envelope[connectionCheck]{
			Target:   &t.Name,
			Result:   connectionCheck{Stages: []stage{server, namespace, codec}},
			Complete: true,
		}, nil
	}
}

// runStage runs check, which returns why it failed or "" if it succeeded.
func runStage(ctx context.Context, name string, check func(context.Context) (reason string)) (stage, error) {
	stageCtx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()
	start := time.Now()
	reason := check(stageCtx)
	if reason != "" && ctx.Err() != nil {
		return stage{}, ctx.Err()
	}
	s := stage{Name: name, Status: "ok", LatencyMS: new(time.Since(start).Milliseconds())}
	if reason != "" {
		s.Status, s.Reason = "failed", &reason
	}
	return s, nil
}

// temporalCheck makes a Temporal call a stage check, whose reason hides the server's address.
func temporalCheck(t *config.Target, call func(context.Context, ...grpc.CallOption) error) func(context.Context) string {
	return func(ctx context.Context) string {
		var p peer.Peer
		if err := call(ctx, grpc.Peer(&p)); err != nil {
			return fmt.Sprintf("%s: %s", serviceerror.ToStatus(err).Code(), rpcErrorMessage(err, t, &p, temporalServer))
		}
		return ""
	}
}

func rpcErrorMessage(err error, t *config.Target, p *peer.Peer, server string) string {
	st := serviceerror.ToStatus(err)
	code := st.Code()
	if code == codes.DeadlineExceeded {
		return "timed out waiting for " + server
	}
	reached := p.Addr != nil
	// Errors raised before a call reaches the server name its address, which results must not reveal.
	switch {
	// Under TLS 1.3 a server rejects a client certificate after the client's side of the handshake, as a remote TLS alert.
	case code == codes.Unavailable && !reached && (strings.Contains(st.Message(), "authentication handshake failed") || strings.Contains(st.Message(), "remote error: tls:")):
		return "the TLS handshake or certificate check with " + server + " failed"
	case code == codes.Unavailable && !reached:
		return "could not connect to " + server
	}
	msg := scrub(st.Message(), t, p)
	if code == codes.Unavailable {
		return server + " reported: " + msg
	}
	return msg
}

func scrub(msg string, t *config.Target, p *peer.Peer) string {
	addrs := []string{t.Address}
	if p.Addr != nil {
		addrs = append(addrs, p.Addr.String())
	}
	for _, addr := range slices.Clone(addrs) {
		stripped, prefixed := strings.CutPrefix(addr, "dns:///")
		if prefixed {
			addrs = append(addrs, stripped)
		}
		if host, _, err := net.SplitHostPort(stripped); err == nil && strings.ContainsAny(host, ".:") {
			addrs = append(addrs, host)
		}
	}
	var pairs []string
	for _, addr := range addrs {
		if addr != "" {
			pairs = append(pairs, addr, "<address>")
		}
	}
	return strings.NewReplacer(pairs...).Replace(msg)
}
