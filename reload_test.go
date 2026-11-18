package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

// missing stands for a config file that cannot be read.
const missing = "<missing>"

type watch struct {
	t     *testing.T
	ticks chan time.Time
	hup   chan os.Signal
	reads chan string
	got   chan string
}

// startWatch runs watchConfig from the contents data and returns a watch that drives it.
func startWatch(t *testing.T, data string) *watch {
	t.Helper()
	w := &watch{t: t, ticks: make(chan time.Time), hup: make(chan os.Signal), reads: make(chan string), got: make(chan string)}
	ctx, cancel := context.WithCancel(t.Context())
	read := func() ([]byte, error) {
		select {
		case data := <-w.reads:
			if data == missing {
				return nil, os.ErrNotExist
			}
			return []byte(data), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	reload := func(data []byte, err error) {
		got := string(data)
		if err != nil {
			got = missing
		}
		select {
		case w.got <- got:
		case <-ctx.Done():
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchConfig(ctx, read, []byte(data), w.ticks, w.hup, reload)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return w
}

// tick has the watcher read data on a tick, failing if it reloads instead of taking the tick.
func (w *watch) tick(data string) {
	w.t.Helper()
	select {
	case w.ticks <- time.Time{}:
	case got := <-w.got:
		w.t.Fatalf("reloaded %q, want no reload", got)
	}
	w.reads <- data
}

// hangUp has the watcher read data on a SIGHUP, failing if it reloads instead of taking the signal.
func (w *watch) hangUp(data string) {
	w.t.Helper()
	select {
	case w.hup <- syscall.SIGHUP:
	case got := <-w.got:
		w.t.Fatalf("reloaded %q, want no reload", got)
	}
	w.reads <- data
}

// reloaded fails unless the watcher reloads want.
func (w *watch) reloaded(want string) {
	w.t.Helper()
	select {
	case got := <-w.got:
		if got != want {
			w.t.Errorf("reloaded %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		w.t.Fatalf("no reload of %q", want)
	}
}

func TestWatchConfig(t *testing.T) {
	w := startWatch(t, "a")
	w.tick("a")
	w.tick("a")
	w.tick("b, half written")
	w.tick("b")
	w.tick("b")
	w.reloaded("b")
	w.tick("b")
	w.tick("b")
	w.hangUp("b")
	w.reloaded("b")

	w.tick(missing)
	w.tick(missing)
	w.reloaded(missing)
	w.tick(missing)
	w.tick("b")
	w.tick("b")
	w.reloaded("b")
	w.tick("b")
	w.tick("b")
}

func TestCheckRestartOnly(t *testing.T) {
	// cfg returns a new config that sets each field the server reads only at startup, edited by change if it is not nil.
	cfg := func(change func(*config.Config)) *config.Config {
		c := &config.Config{
			Listen:       "127.0.0.1:8080",
			ListenTLS:    &config.Certificate{CertFile: "tls.crt", KeyFile: "tls.key"},
			Auth:         &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: "https://idp.example.com", Audience: "a", RequiredScopes: []string{"s"}},
			AuditLogFile: "audit.log",
			ErrorLogFile: "error.log",
			Targets:      []config.Target{{Name: "a"}},
		}
		if change != nil {
			change(c)
		}
		return c
	}
	for _, tt := range []struct {
		name   string
		change func(*config.Config)
		// fields is what checkRestartOnly's error names, or "" if it returns nil.
		fields string
	}{
		{"unchanged", nil, ""},
		{"listen", func(c *config.Config) { c.Listen = "127.0.0.1:8081" }, "listen"},
		{"listen_tls", func(c *config.Config) { c.ListenTLS.KeyFile = "other.key" }, "listen_tls"},
		{"auth", func(c *config.Config) { c.Auth.RequiredScopes = []string{"s", "t"} }, "auth"},
		{"audit_log_file", func(c *config.Config) { c.AuditLogFile = "other.log" }, "audit_log_file"},
		{"error_log_file", func(c *config.Config) { c.ErrorLogFile = "other.log" }, "error_log_file"},
		{"listen and auth", func(c *config.Config) { c.Listen, c.Auth.Audience = "127.0.0.1:8081", "b" }, "listen, auth"},
		{"other fields", func(c *config.Config) {
			c.Targets = append(c.Targets, config.Target{Name: "b"})
			c.Authorization = &config.Authorization{Profiles: map[string]config.Profile{"p": {Targets: []string{"b"}}}}
			c.Codecs = map[string]config.Codec{"codec": {Endpoint: "https://codec.example.com"}}
			c.ShutdownTimeout = time.Second
		}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			switch err := checkRestartOnly(cfg(nil), cfg(tt.change)); {
			case tt.fields == "" && err != nil:
				t.Errorf("checkRestartOnly = %v, want nil", err)
			case tt.fields != "" && (err == nil || !strings.HasPrefix(err.Error(), tt.fields+": changes take effect only when the server restarts")):
				t.Errorf("checkRestartOnly = %v, want an error naming %s", err, tt.fields)
			}
		})
	}
}

func TestTargetChanges(t *testing.T) {
	// a changes only through its codec's endpoint, and e only through its description.
	prev := &config.Config{
		Targets: []config.Target{{Name: "a", Codec: "codec"}, {Name: "b"}, {Name: "c"}, {Name: "e", Description: "x"}},
		Codecs:  map[string]config.Codec{"codec": {Endpoint: "https://one.example.com"}},
	}
	next := &config.Config{
		Targets: []config.Target{{Name: "a", Codec: "codec"}, {Name: "b"}, {Name: "d"}, {Name: "e", Description: "y"}},
		Codecs:  map[string]config.Codec{"codec": {Endpoint: "https://two.example.com"}},
	}
	added, removed, changed := targetChanges(prev, next)
	if !slices.Equal(added, []string{"d"}) || !slices.Equal(removed, []string{"c"}) || !slices.Equal(changed, []string{"a", "e"}) {
		t.Errorf("targetChanges = added %v, removed %v, changed %v; want [d], [c], [a e]", added, removed, changed)
	}
}

func TestOpenTargetsClosesClientsOnFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(gs, &blockingWorkflowService{})
	go gs.Serve(ln)
	t.Cleanup(gs.Stop)
	var made *temporal.Clients
	prev := newTargetClients
	newTargetClients = func(targets []config.Target, n, v string, logger *slog.Logger, opts ...temporal.Option) (*temporal.Clients, error) {
		clients, err := prev(targets, n, v, logger, opts...)
		made = clients
		return clients, err
	}
	t.Cleanup(func() { newTargetClients = prev })
	// b's API key cannot be read, so openTargets fails after it creates the clients.
	t.Setenv("TEMPORAL_MCP_TEST_UNSET_KEY", "")
	cfg, err := config.Parse([]byte(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: %q, namespace: default}\n  - {name: b, description: x, address: \"b:7233\", namespace: default, api_key: {env: TEMPORAL_MCP_TEST_UNSET_KEY}}\n", ln.Addr())))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openTargets(cfg, slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), "TEMPORAL_MCP_TEST_UNSET_KEY") {
		t.Fatalf("openTargets error = %v, want one about b's API key", err)
	}
	if made == nil {
		t.Fatal("openTargets created no clients")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = temporal.WithTool(ctx, "check_connection", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{"a"})
	_, err = made.Client("a").WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if code := serviceerror.ToStatus(err).Code(); code != codes.Canceled {
		t.Errorf("a call through the clients that openTargets created before failing = %v, want Canceled, since it closed them", code)
	}
}
