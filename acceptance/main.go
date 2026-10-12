// Acceptance runs one of the four acceptance sessions, in which a Codex agent investigates a Temporal problem
// through operator-mcp. It builds operator-mcp from this module, starts a Temporal dev server, seeds the
// session's workflows, runs the agent command with the session's scenario, and then stops the workers and the
// dev server. With --noisy, it seeds the workflows of all four sessions, not only the chosen one. With
// --scenario, it renders the template at that path in place of the session's scenario. Like the templates in the
// scenarios directory, the template gets the server's binary, config file and repository root as .Server, .Config
// and .Root, and .Deployed once Session 3 is seeded and .IncidentStart and .IncidentEnd once Session 4 is.
//
// It needs Go, Node, a checkout of github.com/temporalio/agent-driver, and the Codex CLI, logged in.
//
// Usage, from the repository root:
//
//	go run -trimpath ./acceptance --session 1 -- npm --prefix /path/to/agent-driver/interactive-agent-interface run scenario -- '{scenario}' --out /path/to/runs --timeout-ms 300000
//
// It replaces {scenario} with the path of the scenario file, in a temporary directory that it removes on exit, so
// pass --out to keep agent-driver's run artifacts. It runs the agent command without any TEMPORAL_ environment
// variables, so a temporal CLI that the agent runs gets no connection settings from them.
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"text/template"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/temporalio/operator-mcp/internal/fixture"
)

//go:embed scenarios
var scenarios embed.FS

const namespace = "orders-prod"

func main() {
	session := flag.Int("session", 0, "the session to run, 1 to 4 (required)")
	noisy := flag.Bool("noisy", false, "seed the workflows of all four sessions, not only the chosen one")
	scenarioTemplate := flag.String("scenario", "", "path to a scenario template to render instead of the session's own")
	flag.Parse()
	code, err := run(*session, *noisy, *scenarioTemplate, flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "acceptance: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(session int, noisy bool, scenarioTemplate string, command []string) (int, error) {
	if info, ok := debug.ReadBuildInfo(); !ok || !slices.Contains(info.Settings, debug.BuildSetting{Key: "-trimpath", Value: "true"}) {
		return 0, errors.New("run it with go run -trimpath, which keeps this machine's file paths out of the stack traces that the seeded workflows record")
	}
	if session < 1 || session > 4 {
		return 0, errors.New("--session must be 1, 2, 3 or 4")
	}
	if !slices.Contains(command, "{scenario}") {
		return 0, errors.New("the agent command after -- must have a {scenario} argument")
	}
	name := fmt.Sprintf("session-%d.yaml", session)
	tmpl, err := parseScenario(name, scenarioTemplate)
	if err != nil {
		return 0, err
	}
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if !signal.Ignored(syscall.SIGHUP) {
		signals = append(signals, syscall.SIGHUP)
	}
	ctx, stop := signal.NotifyContext(context.Background(), signals...)
	defer stop()
	dir, err := os.MkdirTemp("", "operator-mcp-acceptance-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	root, server, err := buildServer(ctx, dir)
	if err != nil {
		return 0, err
	}
	// Sticky task queues, like the SDK's default identities, are named after this machine's host.
	worker.SetStickyWorkflowCacheSize(0)
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: namespace, Identity: "orders-api", Logger: log.NewStructuredLogger(slog.New(slog.DiscardHandler))},
		ExtraArgs:     []string{"--search-attribute", "OrderId=Keyword", "--search-attribute", "Team=Keyword", "--search-attribute", "Domain=Keyword"},
		Stdout:        os.Stderr,
	})
	if err != nil {
		return 0, err
	}
	defer dev.Stop()
	config, err := writeConfig(dir, dev.FrontendHostPort())
	if err != nil {
		return 0, err
	}

	values := map[string]string{"Server": server, "Config": config, "Root": root}
	seeded := []int{session}
	if noisy {
		// Session 4's incident window ends before the others' workflows start, and Session 3's deployment follows them.
		seeded = []int{4, 1, 2, 3}
	}
	for _, s := range seeded {
		fmt.Fprintf(os.Stderr, "acceptance: seeding session %d on the dev server at %s\n", s, dev.FrontendHostPort())
		stopWorkers, err := seed(ctx, dev.Client(), s, values)
		if stopWorkers != nil {
			defer stopWorkers()
		}
		if err != nil {
			return 0, fmt.Errorf("seeding session %d: %w", s, err)
		}
	}

	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, values); err != nil {
		return 0, err
	}
	scenario := filepath.Join(dir, name)
	if err := os.WriteFile(scenario, rendered.Bytes(), 0o600); err != nil {
		return 0, err
	}
	err = agentCommand(ctx, command, scenario).Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() > 0 {
		return exit.ExitCode(), nil
	}
	return 0, err
}

func agentCommand(ctx context.Context, command []string, scenario string) *exec.Cmd {
	args := slices.Clone(command)
	for i, arg := range args {
		if arg == "{scenario}" {
			args[i] = scenario
		}
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "TEMPORAL_") })
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	return cmd
}

func buildServer(ctx context.Context, dir string) (root, bin string, err error) {
	list := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.Dir}}")
	list.Stderr = os.Stderr
	out, err := list.Output()
	if err != nil {
		return "", "", fmt.Errorf("go list: %w", err)
	}
	root, bin = strings.TrimSpace(string(out)), filepath.Join(dir, "operator-mcp")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, ".")
	build.Dir, build.Stderr = root, os.Stderr
	if err := build.Run(); err != nil {
		return "", "", fmt.Errorf("go build: %w", err)
	}
	return root, bin, nil
}

func writeConfig(dir, address string) (string, error) {
	path := filepath.Join(dir, "config.yaml")
	config := fmt.Sprintf("targets:\n"+
		"  - name: orders-prod\n"+
		"    aliases: [orders prod]\n"+
		"    description: Production namespace for orders, payments, fulfillment and the ledger.\n"+
		"    address: %q\n"+
		"    namespace: %s\n"+
		"    payload_handling: passthrough\n", address, namespace)
	return path, os.WriteFile(path, []byte(config), 0o600)
}

func seed(ctx context.Context, c client.Client, session int, values map[string]string) (stop func(), err error) {
	switch session {
	case 1:
		w := fixture.NewWorker(c)
		if err := w.Start(); err != nil {
			return nil, err
		}
		_, err := fixture.SeedOrders(ctx, c, 20)
		return w.Stop, err
	case 2:
		return fixture.SeedStuckLedger(ctx, c, namespace)
	case 3:
		deployed, stop, err := fixture.SeedFulfillment(ctx, c)
		values["Deployed"] = deployed.UTC().Format(time.RFC3339)
		return stop, err
	default:
		start, end, stop, err := fixture.SeedIncident(ctx, c, 20)
		values["IncidentStart"], values["IncidentEnd"] = start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339)
		return stop, err
	}
}

func parseScenario(name, path string) (*template.Template, error) {
	text, err := scenarios.ReadFile("scenarios/" + name)
	if path != "" {
		name = path
		text, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	return template.New(name).Option("missingkey=error").Funcs(template.FuncMap{"json": func(s string) string {
		b, _ := json.Marshal(s)
		return string(b)
	}}).Parse(string(text))
}
