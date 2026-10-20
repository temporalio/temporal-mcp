// Temporal-operator-mcp runs an MCP server that gives AI agents read access to
// Temporal targets.
package main

import (
	"cmp"
	"context"
	"crypto/tls"
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

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/payload"
	"github.com/temporalio/temporal-operator-mcp/internal/server"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

const (
	name       = "temporal-operator-mcp"
	serverName = "temporal-operator"
)

var version = "dev"

// cancelGrace is how long the tool calls still running at the shutdown timeout
// have, once cancelled, to end and write their audit records.
const cancelGrace = time.Second

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.ReadCloser, stdout io.WriteCloser, stderr io.Writer) error {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the YAML config file (required)")
	stdio := flags.Bool("stdio", false, "serve MCP over stdin and stdout, for an MCP client that launches the server, rather than over HTTP")
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

	load := config.Load
	if *stdio {
		// Broken client pipes must not stop calls before they are audited.
		signal.Ignore(syscall.SIGPIPE)
		load = config.LoadStdio
	}
	cfg, err := load(*configPath)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	// Over stdio, stdout carries only MCP messages, so the audit records default to stderr.
	var records io.Writer = stdout
	if *stdio {
		records = stderr
	}
	records, err = openLog(cfg.AuditLogFile, records)
	if err != nil {
		return fmt.Errorf("invalid config %s: audit_log_file: %w", *configPath, err)
	}
	logs, err := openLog(cfg.ErrorLogFile, stderr)
	if err != nil {
		return fmt.Errorf("invalid config %s: error_log_file: %w", *configPath, err)
	}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	clients, err := temporal.NewClients(cfg.Targets, name, version, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	payloads, err := payload.NewRenderer(cfg, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	// Cancelling calls cancels the tool calls in progress.
	calls, cancelCalls := context.WithCancel(context.Background())
	defer cancelCalls()
	subscriptions, cancelSubscriptions := context.WithCancel(context.Background())
	defer cancelSubscriptions()
	s, err := server.NewServer(calls, subscriptions, serverName, version, cfg, clients, payloads, records, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	timeout := cmp.Or(cfg.ShutdownTimeout, config.DefaultShutdownTimeout)
	if *stdio {
		transport := &stdioTransport{
			IOTransport: mcp.IOTransport{Reader: stdin, Writer: stdout},
			eof:         make(chan struct{}), drained: make(chan struct{}), closed: make(chan struct{}),
		}
		session, err := s.Connect(context.Background(), transport, nil)
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- session.Wait() }()
		select {
		case err := <-done:
			return err
		case <-signals.Done():
		case <-transport.eof:
		}
		// A second signal stops the process at once, including after stdin closes.
		stop()
		cancelSubscriptions()
		go func() {
			transport.drain()
			_ = session.Close()
		}()
		return shutdown(func(ctx context.Context) error {
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}, timeout, cancelCalls, stderr, logger)
	}

	srv := &http.Server{Handler: server.NewMux(s, cfg, logger), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn)}
	scheme := "http"
	if cfg.ListenTLS != nil {
		if err := cfg.ListenTLS.Check(logger); err != nil {
			return fmt.Errorf("invalid config %s: listen_tls: %w", *configPath, err)
		}
		srv.TLSConfig = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := cfg.ListenTLS.Load()
			if err != nil {
				logger.Error("cannot load listen_tls certificate", "error", err.Error())
			}
			return cert, err
		}}
		scheme = "https"
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%s: listening on %s://%s/mcp\n", name, scheme, ln.Addr())

	served := make(chan error, 1)
	go func() {
		if srv.TLSConfig != nil {
			served <- srv.ServeTLS(ln, "", "")
			return
		}
		served <- srv.Serve(ln)
	}()
	select {
	case err := <-served:
		return err
	case <-signals.Done():
	}
	// A second signal stops the process at once.
	stop()
	cancelSubscriptions()
	return shutdown(srv.Shutdown, timeout, cancelCalls, stderr, logger)
}

// shutdown waits up to timeout for requests to drain, then cancels those still
// running and waits up to cancelGrace for them to end and write their audit records.
func shutdown(drain func(context.Context) error, timeout time.Duration, cancelCalls context.CancelFunc, stderr io.Writer, logger *slog.Logger) error {
	fmt.Fprintf(stderr, "%s: shutting down\n", name)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	logger.Warn("cancelling the requests still in progress at the shutdown timeout", "shutdown_timeout", timeout)
	cancelCalls()
	ctx, cancel = context.WithTimeout(context.Background(), cancelGrace)
	defer cancel()
	if err := drain(ctx); err != nil {
		return fmt.Errorf("requests still in progress %s after they were cancelled", cancelGrace)
	}
	return nil
}

func openLog(path string, fallback io.Writer) (io.Writer, error) {
	if path == "" {
		return fallback, nil
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
}
