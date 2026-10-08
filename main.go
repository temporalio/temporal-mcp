// Operator-mcp runs an MCP server that gives AI agents read access to Temporal
// targets.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/payload"
	"github.com/temporalio/operator-mcp/internal/server"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const (
	name       = "operator-mcp"
	serverName = "temporal-operator"
)

var version = "dev"

// cancelGrace is how long the tool calls still running at the shutdown timeout
// have, once cancelled, to end and write their audit records.
const cancelGrace = time.Second

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the YAML config file (required)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", flags.Args())
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	clients, err := temporal.NewClients(cfg.Targets, name, version, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	payloads, err := payload.NewRenderer(cfg, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	handler, err := server.NewMux(serverName, version, cfg, clients, payloads, stdout, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%s: listening on http://%s/mcp\n", name, ln.Addr())

	// Every request's context derives from calls, so cancelling it ends the tool calls in progress.
	calls, cancelCalls := context.WithCancel(context.Background())
	defer cancelCalls()
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return calls },
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		return err
	case <-signals.Done():
	}
	// A second signal stops the process at once.
	stop()
	return shutdown(srv, cmp.Or(cfg.ShutdownTimeout, config.DefaultShutdownTimeout), cancelCalls, stderr, logger)
}

// shutdown stops srv accepting connections and waits up to timeout for the
// requests in progress to end. It then cancels those still running, which ends
// their tool calls as cancelled, and waits up to cancelGrace for them to end.
func shutdown(srv *http.Server, timeout time.Duration, cancelCalls context.CancelFunc, stderr io.Writer, logger *slog.Logger) error {
	fmt.Fprintf(stderr, "%s: shutting down\n", name)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	logger.Warn("cancelling the requests still in progress at the shutdown timeout", "shutdown_timeout", timeout)
	cancelCalls()
	ctx, cancel = context.WithTimeout(context.Background(), cancelGrace)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("requests still in progress %s after they were cancelled", cancelGrace)
	}
	return nil
}
