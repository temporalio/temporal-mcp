package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/temporalio/temporal-mcp/internal/config"
)

func TestWatchConfig(t *testing.T) {
	// At each step, watchConfig gets a tick, or a SIGHUP with hup, reads read, or an unreadable file if read is "",
	// and then reloads reload, or does not reload if reload is "".
	steps := []struct {
		read   string
		hup    bool
		reload string
	}{
		{read: "a"}, {read: "b, half written"}, {read: "b"}, {read: "b", reload: "b"}, {read: "b"},
		{read: "b", hup: true, reload: "b"},
		{read: ""}, {read: "", reload: "unreadable"}, {read: ""}, {read: "b"}, {read: "b", reload: "b"},
	}
	ticks, hups, reloads := make(chan time.Time), make(chan os.Signal), make(chan string)
	reads := 0
	read := func() ([]byte, error) {
		reads++
		if steps[reads-1].read == "" {
			return nil, os.ErrNotExist
		}
		return []byte(steps[reads-1].read), nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go watchConfig(ctx, read, []byte("a"), ticks, hups, func(data []byte, err error) {
		if err != nil {
			data = []byte("unreadable")
		}
		reloads <- string(data)
	})
	for i, step := range steps {
		tick, hup := ticks, hups
		if step.hup {
			tick = nil
		} else {
			hup = nil
		}
		select {
		case tick <- time.Time{}:
		case hup <- syscall.SIGHUP:
		case got := <-reloads:
			t.Fatalf("before step %d, reloaded %q", i, got)
		}
		if step.reload == "" {
			continue
		}
		select {
		case got := <-reloads:
			if got != step.reload {
				t.Errorf("step %d reloaded %q, want %q", i, got, step.reload)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("step %d did not reload %q", i, step.reload)
		}
	}
}

func TestCheckRestartOnly(t *testing.T) {
	// cfg returns a config that sets each field the server reads only at startup, edited by change.
	cfg := func(change func(*config.Config)) *config.Config {
		c := &config.Config{
			Listen:       "127.0.0.1:8080",
			ListenTLS:    &config.Certificate{CertFile: "tls.crt", KeyFile: "tls.key"},
			Auth:         &config.Auth{Resource: "https://mcp.example.com/mcp", Audience: "a"},
			AuditLogFile: "audit.log",
			ErrorLogFile: "error.log",
		}
		change(c)
		return c
	}
	for _, tt := range []struct {
		change func(*config.Config)
		want   string
	}{
		{func(c *config.Config) { c.ListenTLS.KeyFile = "other.key" }, "listen_tls: "},
		{func(c *config.Config) { c.Auth.Audience = "b" }, "auth: "},
		{func(c *config.Config) { c.AuditLogFile = "other.log" }, "audit_log_file: "},
		{func(c *config.Config) { c.ErrorLogFile = "other.log" }, "error_log_file: "},
		{func(c *config.Config) { c.Listen, c.Auth.Audience = "127.0.0.1:8081", "b" }, "listen, auth: "},
		{func(c *config.Config) {
			c.Targets, c.Authorization, c.ShutdownTimeout = []config.Target{{Name: "b"}}, &config.Authorization{}, time.Second
		}, "<nil>"},
	} {
		if got := fmt.Sprint(checkRestartOnly(cfg(func(*config.Config) {}), cfg(tt.change))); !strings.HasPrefix(got, tt.want) {
			t.Errorf("checkRestartOnly = %s, want %s...", got, tt.want)
		}
	}
}
