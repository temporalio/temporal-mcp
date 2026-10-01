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

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const stageTimeout = 5 * time.Second

type checkConnectionInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target from list_available_targets"`
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

func checkConnection(cfg *config.Config, clients *temporal.Clients) mcp.ToolHandlerFor[checkConnectionInput, envelope[connectionCheck]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in checkConnectionInput) (*mcp.CallToolResult, envelope[connectionCheck], error) {
		t, err := lookupTarget(cfg, in.Target)
		if err != nil {
			return nil, envelope[connectionCheck]{}, err
		}
		svc := clients.Client(t.Name).WorkflowService()
		server := runStage(ctx, "server", t, func(ctx context.Context, opts ...grpc.CallOption) error {
			_, err := svc.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}, opts...)
			return err
		})
		namespace := stage{Name: "namespace", Status: "skipped", Reason: new("not run because the server stage failed")}
		if server.Status == "ok" {
			namespace = runStage(ctx, "namespace", t, func(ctx context.Context, opts ...grpc.CallOption) error {
				_, err := svc.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: t.Namespace}, opts...)
				return err
			})
		}
		return nil, envelope[connectionCheck]{
			Target:   &t.Name,
			Result:   connectionCheck{Stages: []stage{server, namespace}},
			Complete: true,
		}, nil
	}
}

func runStage(ctx context.Context, name string, t *config.Target, call func(context.Context, ...grpc.CallOption) error) stage {
	ctx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()
	var p peer.Peer
	start := time.Now()
	err := call(ctx, grpc.Peer(&p))
	s := stage{Name: name, Status: "ok", LatencyMS: new(time.Since(start).Milliseconds())}
	if err != nil {
		s.Status, s.Reason = "failed", new(fmt.Sprintf("%s: %s", serviceerror.ToStatus(err).Code(), rpcErrorMessage(err, t, &p)))
	}
	return s
}

func rpcErrorMessage(err error, t *config.Target, p *peer.Peer) string {
	st := serviceerror.ToStatus(err)
	code := st.Code()
	if code == codes.DeadlineExceeded {
		return "timed out waiting for the Temporal server"
	}
	reached := p.Addr != nil
	// Errors raised before a call reaches the Temporal server name its address, which results must not reveal.
	switch {
	case code == codes.Unavailable && !reached && strings.Contains(st.Message(), "authentication handshake failed"):
		return "the TLS handshake or certificate check with the Temporal server failed"
	case code == codes.Unavailable && !reached:
		return "could not connect to the Temporal server"
	}
	msg := scrub(st.Message(), t, p)
	if code == codes.Unavailable {
		return "the Temporal server reported: " + msg
	}
	return msg
}

func scrub(msg string, t *config.Target, p *peer.Peer) string {
	addrs := []string{t.Address}
	if p.Addr != nil {
		addrs = append(addrs, p.Addr.String())
	}
	for _, addr := range slices.Clone(addrs) {
		if host, _, err := net.SplitHostPort(addr); err == nil && strings.ContainsAny(host, ".:") {
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
