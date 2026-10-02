// Package config loads and validates the server configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Config is the server configuration.
type Config struct {
	// Listen is the host:port the HTTP server binds to. The host must be a loopback IP.
	Listen  string   `yaml:"listen"`
	Targets []Target `yaml:"targets"`
}

// Target is a named Temporal namespace at an address that the server can read from.
type Target struct {
	Name        string   `yaml:"name"`
	Aliases     []string `yaml:"aliases"`
	Description string   `yaml:"description"`
	Address     string   `yaml:"address"`
	Namespace   string   `yaml:"namespace"`
	// APIKey says where to read the target's API key. With one the server connects
	// over TLS; without one it connects in plaintext with no credentials.
	APIKey *APIKey `yaml:"api_key"`
}

// APIKey says where to read a Temporal API key from. Exactly one field is set.
type APIKey struct {
	// Env is the name of an environment variable holding the key, read at startup.
	Env string `yaml:"env"`
	// File is the path of a file holding the key, read on every Temporal call.
	File string `yaml:"file"`
}

// Load reads the YAML file at path and returns the validated configuration.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(data)
}

func parse(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("config is empty")
		}
		return nil, err
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("config must contain exactly one YAML document")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	var errs []error
	if err := validateListen(c.Listen); err != nil {
		errs = append(errs, err)
	}
	if len(c.Targets) == 0 {
		errs = append(errs, errors.New("targets: at least one target is required"))
	}
	names := map[string]int{}
	type seenAlias struct {
		alias  string
		target int
	}
	var aliases []seenAlias
	for i, t := range c.Targets {
		label := fmt.Sprintf("targets[%d]", i)
		for _, f := range []struct{ name, value string }{{"name", t.Name}, {"description", t.Description}, {"address", t.Address}, {"namespace", t.Namespace}} {
			if strings.TrimSpace(f.value) == "" {
				errs = append(errs, fmt.Errorf("%s: %s is required", label, f.name))
			}
		}
		if strings.TrimSpace(t.Name) != "" {
			if j, ok := names[t.Name]; ok {
				errs = append(errs, fmt.Errorf("%s: name %q conflicts with the name of targets[%d]", label, t.Name, j))
			} else {
				names[t.Name] = i
			}
		}
		for k, alias := range t.Aliases {
			if strings.TrimSpace(alias) == "" {
				errs = append(errs, fmt.Errorf("%s: aliases[%d] is empty", label, k))
				continue
			}
			for j, u := range c.Targets {
				if strings.EqualFold(alias, u.Name) {
					errs = append(errs, fmt.Errorf("%s: alias %q conflicts with name %q of %s; aliases are case-insensitive", label, alias, u.Name, targetRef(i, j)))
					break
				}
			}
			for _, s := range aliases {
				if strings.EqualFold(alias, s.alias) {
					errs = append(errs, fmt.Errorf("%s: alias %q conflicts with alias %q of %s; aliases are case-insensitive", label, alias, s.alias, targetRef(i, s.target)))
					break
				}
			}
			aliases = append(aliases, seenAlias{alias, i})
		}
		if k := t.APIKey; k != nil && (k.Env == "") == (k.File == "") {
			errs = append(errs, fmt.Errorf("%s: api_key: exactly one of env or file is required", label))
		}
	}
	return errors.Join(errs...)
}

// Lookup returns the target whose name is exactly name or that has an alias equal to name ignoring case.
func (c *Config) Lookup(name string) (*Target, bool) {
	for i := range c.Targets {
		if c.Targets[i].Name == name {
			return &c.Targets[i], true
		}
	}
	for i, t := range c.Targets {
		for _, alias := range t.Aliases {
			if strings.EqualFold(alias, name) {
				return &c.Targets[i], true
			}
		}
	}
	return nil, false
}

func targetRef(from, to int) string {
	if from == to {
		return "this target"
	}
	return fmt.Sprintf("targets[%d]", to)
}

func validateListen(listen string) error {
	if listen == "" {
		return errors.New("listen is required")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !addr.IsLoopback() {
		return fmt.Errorf("listen: host %q must be a loopback IP address such as 127.0.0.1 or ::1, because the server has no authentication", host)
	}
	return nil
}
