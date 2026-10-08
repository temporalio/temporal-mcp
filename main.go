// Operator-mcp runs an MCP server that gives AI agents read access to Temporal
// targets.
package main

import (
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

	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn)}
	scheme := "http"
	if cfg.ListenTLS != nil {
		if _, err := tls.LoadX509KeyPair(cfg.ListenTLS.CertFile, cfg.ListenTLS.KeyFile); err != nil {
			return fmt.Errorf("invalid config %s: listen_tls: %w", *configPath, err)
		}
		srv.TLSConfig = &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(cfg.ListenTLS.CertFile, cfg.ListenTLS.KeyFile)
			if err != nil {
				logger.Error("cannot load listen_tls certificate", "error", err.Error())
				return nil, err
			}
			return &cert, nil
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
