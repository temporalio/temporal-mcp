// Operator-mcp runs an MCP server that gives AI agents read access to Temporal
// targets.
package main

import (
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
	"github.com/temporalio/operator-mcp/internal/server"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const (
	name       = "operator-mcp"
	serverName = "temporal-operator"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
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

	clients, err := temporal.NewClients(cfg.Targets, name, version)
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	handler, err := server.NewMux(serverName, version, cfg, clients, slog.New(slog.NewTextHandler(stderr, nil)))
	if err != nil {
		return fmt.Errorf("invalid config %s: %w", *configPath, err)
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%s: listening on http://%s/mcp\n", name, ln.Addr())

	return (&http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}).Serve(ln)
}
