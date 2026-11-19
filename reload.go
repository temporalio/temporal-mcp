package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/server"
)

// configPollInterval is how often --watch-config reads the config file.
const configPollInterval = 2 * time.Second

// watchConfig calls reload with what read returns, the config file's contents
// or the error reading it: at once on each receive from hup, and on a receive
// from ticks once what read returns matches the previous read and differs from
// what reload last got, data at first. It returns once ctx is done.
func watchConfig(ctx context.Context, read func() ([]byte, error), data []byte, ticks <-chan time.Time, hup <-chan os.Signal, reload func([]byte, error)) {
	type contents struct {
		data []byte
		err  error
	}
	same := func(a, b contents) bool { return bytes.Equal(a.data, b.data) && fmt.Sprint(a.err) == fmt.Sprint(b.err) }
	last := contents{data: data}
	seen := last
	for {
		var c contents
		select {
		case <-ctx.Done():
			return
		case <-hup:
			c.data, c.err = read()
		case <-ticks:
			c.data, c.err = read()
			// A file read mid-write can still be valid YAML, so a change counts once two reads agree.
			stable := same(c, seen)
			seen = c
			if !stable || same(c, last) {
				continue
			}
		}
		seen, last = c, c
		reload(c.data, c.err)
	}
}

// reloadConfig returns watchConfig's reload function, which parses the contents,
// refuses a change to a restart-only field, gives s the config with new clients
// and stores it in current, logging the outcome.
func reloadConfig(s *server.Server, current *atomic.Pointer[config.Config], parse func([]byte) (*config.Config, error), logger *slog.Logger) func([]byte, error) {
	apply := func(data []byte) error {
		prev := current.Load()
		cfg, err := parse(data)
		if err != nil {
			return err
		}
		if err := checkRestartOnly(prev, cfg); err != nil {
			return err
		}
		clients, payloads, err := openTargets(cfg, logger)
		if err != nil {
			return err
		}
		if err := s.Reload(cfg, clients, payloads); err != nil {
			return err
		}
		current.Store(cfg)
		added, removed, changed := targetChanges(prev, cfg)
		logger.Info("reloaded the config", "added", added, "removed", removed, "changed", changed)
		return nil
	}
	return func(data []byte, err error) {
		if err == nil {
			err = apply(data)
		}
		if err != nil {
			logger.Error("cannot reload the config, so the server keeps its last good config", "error", err.Error())
		}
	}
}

// checkRestartOnly fails if next changes a field of prev that the server reads only at startup.
func checkRestartOnly(prev, next *config.Config) error {
	var fields []string
	for _, f := range []struct {
		name       string
		prev, next any
	}{
		{"listen", prev.Listen, next.Listen},
		{"listen_tls", prev.ListenTLS, next.ListenTLS},
		{"auth", prev.Auth, next.Auth},
		{"audit_log_file", prev.AuditLogFile, next.AuditLogFile},
		{"error_log_file", prev.ErrorLogFile, next.ErrorLogFile},
	} {
		if !reflect.DeepEqual(f.prev, f.next) {
			fields = append(fields, f.name)
		}
	}
	if len(fields) > 0 {
		return fmt.Errorf("%s: changes take effect only when the server restarts; restart it to apply the config, or undo them to reload the rest", strings.Join(fields, ", "))
	}
	return nil
}

// targetChanges returns the names of the targets that next adds to prev, that
// it removes, and that it changes, including through the codec they use.
func targetChanges(prev, next *config.Config) (added, removed, changed []string) {
	for _, t := range next.Targets {
		switch i := slices.IndexFunc(prev.Targets, func(p config.Target) bool { return p.Name == t.Name }); {
		case i < 0:
			added = append(added, t.Name)
		case !reflect.DeepEqual(prev.Targets[i], t) || !reflect.DeepEqual(prev.Codecs[t.Codec], next.Codecs[t.Codec]):
			changed = append(changed, t.Name)
		}
	}
	for _, p := range prev.Targets {
		if !slices.ContainsFunc(next.Targets, func(t config.Target) bool { return t.Name == p.Name }) {
			removed = append(removed, p.Name)
		}
	}
	return added, removed, changed
}
