// Operator-mcp runs an MCP server that gives AI agents read access to Temporal
// targets.
package main

import (
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
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

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
		load = config.LoadStdio
	}
	cfg, err := load(*configPath)
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

	// Over stdio, stdout carries only MCP messages, so the audit records go to stderr with the logs.
	var records io.Writer = stdout
	if *stdio {
		records = stderr
	}
	s, err := server.NewServer(serverName, version, cfg, clients, payloads, records, logger)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}
	if *stdio {
		return s.Run(context.Background(), &mcp.IOTransport{Reader: stdin, Writer: stdout})
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

	if srv.TLSConfig != nil {
		return srv.ServeTLS(ln, "", "")
	}
	return srv.Serve(ln)
}
