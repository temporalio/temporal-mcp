// Package config loads and validates the server configuration file.
package config

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the server configuration.
type Config struct {
	// Listen is the host:port the HTTP server binds to. Without Auth, the host must be a loopback IP.
	Listen string `yaml:"listen"`
	// ListenTLS, when set, makes the server serve HTTPS on Listen rather than plain HTTP.
	ListenTLS *Certificate `yaml:"listen_tls"`
	Targets   []Target     `yaml:"targets"`
	// Codecs are the codec servers that targets name in their codec field, by name.
	Codecs map[string]Codec `yaml:"codecs"`
	// Auth, when set, requires every MCP request to carry a valid access token.
	Auth *Auth `yaml:"auth"`
	// Authorization decides which tools each caller may use on which targets. It is required with Auth and not allowed without it.
	Authorization *Authorization `yaml:"authorization"`
	// AuditLogFile is the path of the file the server appends audit records to; stdout, or stderr over stdio, if empty.
	AuditLogFile string `yaml:"audit_log_file"`
	// ErrorLogFile is the path of the file the server appends its logs to; stderr if empty.
	ErrorLogFile string `yaml:"error_log_file"`
}

// Certificate is a certificate and its private key, in files that are read again
// for every TLS handshake, so a renewed certificate takes effect for new
// connections without a restart.
type Certificate struct {
	// CertFile is the path of a PEM certificate chain, leaf first.
	CertFile string `yaml:"cert_file"`
	// KeyFile is the path of the certificate's PEM private key.
	KeyFile string `yaml:"key_file"`
}

// Load reads the certificate and its key from their files.
func (c *Certificate) Load() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

// Check loads the certificate and its key, failing if they cannot be loaded,
// and logs a warning to logger if the certificate has expired.
func (c *Certificate) Check(logger *slog.Logger) error {
	cert, err := c.Load()
	if err != nil {
		return err
	}
	if time.Now().After(cert.Leaf.NotAfter) {
		logger.Warn("certificate has expired, so TLS handshakes with it fail until it is renewed", "cert_file", c.CertFile, "not_after", cert.Leaf.NotAfter)
	}
	return nil
}

// Authorization gives each caller the profiles of the rules its access token
// matches. A caller that matches no rule may use no tool on any target.
type Authorization struct {
	Profiles map[string]Profile `yaml:"profiles"`
	Rules    []Rule             `yaml:"rules"`
}

// Profile is permission to use each of Tools on each of Targets, which are target names.
type Profile struct {
	Tools   []string `yaml:"tools"`
	Targets []string `yaml:"targets"`
}

// Rule gives Profile to callers whose access token has the top-level claim
// named ClaimName as a string equal to ClaimValue, or as an array with a string
// element equal to it. A string claim named scope or scp counts as an array of
// its space-separated entries. With Issuer set, the token must also come from
// that issuer.
type Rule struct {
	ClaimName  string `yaml:"claim_name"`
	ClaimValue string `yaml:"claim_value"`
	Issuer     string `yaml:"issuer"`
	Profile    string `yaml:"profile"`
}

// Auth configures the server as an OAuth resource server that accepts JWT access tokens from the configured issuers.
type Auth struct {
	// Resource is the URL at which clients reach this server's MCP endpoint.
	Resource string   `yaml:"resource"`
	Issuers  []Issuer `yaml:"issuers"`
	// AuthorizationServers are the authorization servers advertised to clients; the issuers if empty.
	AuthorizationServers []string `yaml:"authorization_servers"`
	// ScopesSupported are the scopes advertised to clients. They do not grant or deny access.
	ScopesSupported []string `yaml:"scopes_supported"`
}

// Issuer is an identity provider whose access tokens the server accepts.
type Issuer struct {
	// Issuer is the exact iss claim of the provider's tokens.
	Issuer string `yaml:"issuer"`
	// Audience is the value the aud claim of a token for this server must contain.
	// It must identify this server as an API, such as its resource URL or API
	// identifier, never the client app users sign in with: ID tokens issued to
	// that app have the same aud and would pass as access tokens.
	Audience string `yaml:"audience"`
}

// Target is a named Temporal namespace at an address that the server can read from.
type Target struct {
	Name        string   `yaml:"name"`
	Aliases     []string `yaml:"aliases"`
	Description string   `yaml:"description"`
	Address     string   `yaml:"address"`
	Namespace   string   `yaml:"namespace"`
	// APIKey says where to read the target's API key.
	APIKey *Secret `yaml:"api_key"`
	// TLS is the client certificate the server presents to the target instead of
	// an API key. With either, the server connects over TLS; with neither it
	// connects in plaintext with no credentials.
	TLS *Certificate `yaml:"tls"`
	// PayloadHandling says how tool responses show the target's payloads:
	// PayloadOmit, PayloadPassthrough or PayloadDecode. Empty means PayloadOmit.
	PayloadHandling string `yaml:"payload_handling"`
	// Codec is the name of the codec server in Config.Codecs that decodes the
	// target's payloads. It is required when PayloadHandling is PayloadDecode.
	Codec string `yaml:"codec"`
}

// The values of Target.PayloadHandling.
const (
	// PayloadOmit leaves each payload's data out of tool responses, showing only its
	// encoding and size.
	PayloadOmit = "omit"
	// PayloadPassthrough shows each payload as stored: as JSON if it is JSON or
	// null, and otherwise in base64.
	PayloadPassthrough = "passthrough"
	// PayloadDecode shows each payload as the target's codec server decodes it.
	PayloadDecode = "decode"
)

// Codec is a codec server, which decodes payloads over HTTP for the Temporal UI and CLI.
type Codec struct {
	// Endpoint is the codec server's URL, without the /decode path. The
	// placeholder {namespace} in it stands for the namespace of the target.
	Endpoint string `yaml:"endpoint"`
	// AllowPlainHTTP allows an http Endpoint on any host, not only a loopback one.
	AllowPlainHTTP bool `yaml:"allow_plain_http"`
	// AuthorizationHeader says where to read the whole value of the Authorization
	// header sent to the codec server, such as "Bearer abc". Without one, requests
	// carry no Authorization header.
	AuthorizationHeader *Secret `yaml:"authorization_header"`
}

// Secret says where to read a secret from. Exactly one field is set.
type Secret struct {
	// Env is the name of an environment variable holding the secret, read at startup.
	Env string `yaml:"env"`
	// File is the path of a file holding the secret, read on every use.
	File string `yaml:"file"`
}

// Reader returns a function that returns the secret, trimmed of surrounding
// whitespace. It reads an environment variable now, failing if it is unset or
// empty, and a file on every call, failing if it cannot be read or is empty.
func (s *Secret) Reader() (func() (string, error), error) {
	if s.Env != "" {
		v := strings.TrimSpace(os.Getenv(s.Env))
		if v == "" {
			return nil, fmt.Errorf("environment variable %s is unset or empty", s.Env)
		}
		return func() (string, error) { return v, nil }, nil
	}
	return func() (string, error) {
		data, err := os.ReadFile(s.File)
		if err != nil {
			return "", err
		}
		v := strings.TrimSpace(string(data))
		if v == "" {
			return "", fmt.Errorf("%s is empty", s.File)
		}
		return v, nil
	}, nil
}

// Load reads the YAML file at path and returns the validated configuration for
// serving MCP over HTTP.
func Load(path string) (*Config, error) {
	return load(path, parse)
}

// LoadStdio reads the YAML file at path and returns the validated configuration
// for serving MCP over stdin and stdout, which must not have the sections that
// configure HTTP.
func LoadStdio(path string) (*Config, error) {
	return load(path, parseStdio)
}

func load(path string, parse func([]byte) (*Config, error)) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(data)
}

func parse(data []byte) (*Config, error) {
	return decode(data, func(c *Config, _ *keyNodes) error { return errors.Join(c.validateHTTP(), nonStringRuleValues(data)) })
}

func parseStdio(data []byte) (*Config, error) {
	return decode(data, func(_ *Config, k *keyNodes) error { return httpOnlyKeys(k) })
}

// keyNodes holds the YAML nodes of the keys whose presence matters, even with
// no value. A missing key's node has Kind 0.
type keyNodes struct {
	Listen        yaml.Node `yaml:"listen"`
	ListenTLS     yaml.Node `yaml:"listen_tls"`
	Auth          yaml.Node `yaml:"auth"`
	Authorization yaml.Node `yaml:"authorization"`
	Targets       []struct {
		APIKey yaml.Node `yaml:"api_key"`
		TLS    yaml.Node `yaml:"tls"`
	} `yaml:"targets"`
}

// decode decodes data and validates it with validate and validateTargets.
func decode(data []byte, validate func(*Config, *keyNodes) error) (*Config, error) {
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
	// A key with no value decodes to nil; validate it as an empty section rather than ignore it.
	var keys keyNodes
	if yaml.Unmarshal(data, &keys) == nil {
		emptyIfPresent(&cfg.ListenTLS, keys.ListenTLS)
		emptyIfPresent(&cfg.Auth, keys.Auth)
		emptyIfPresent(&cfg.Authorization, keys.Authorization)
		for i, t := range keys.Targets {
			emptyIfPresent(&cfg.Targets[i].APIKey, t.APIKey)
			emptyIfPresent(&cfg.Targets[i].TLS, t.TLS)
		}
	}
	if err := errors.Join(validate(&cfg, &keys), cfg.validateTargets()); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func nonStringRuleValues(data []byte) error {
	var doc struct {
		Authorization struct {
			Rules []struct {
				ClaimValue yaml.Node `yaml:"claim_value"`
			}
		}
	}
	if yaml.Unmarshal(data, &doc) != nil {
		return nil
	}
	var errs []error
	for i, r := range doc.Authorization.Rules {
		if v := r.ClaimValue; v.Kind == yaml.ScalarNode && v.ShortTag() != "!!str" && v.ShortTag() != "!!null" {
			errs = append(errs, fmt.Errorf("authorization: rules[%d]: claim_value: %s is not a string, and rules match only string claim values; quote it to match the string %q", i, v.Value, v.Value))
		}
	}
	return errors.Join(errs...)
}

func emptyIfPresent[T any](section **T, key yaml.Node) {
	if *section == nil && key.Kind != 0 {
		*section = new(T)
	}
}

// httpOnlyKeys rejects the sections that configure HTTP, which serving MCP over stdio has no use for.
func httpOnlyKeys(k *keyNodes) error {
	var keys []string
	for _, key := range []struct {
		name string
		node yaml.Node
	}{{"listen", k.Listen}, {"listen_tls", k.ListenTLS}, {"auth", k.Auth}, {"authorization", k.Authorization}} {
		if key.node.Kind != 0 {
			keys = append(keys, key.name)
		}
	}
	if len(keys) > 0 {
		return fmt.Errorf("%s: not allowed with --stdio, which serves MCP over stdin and stdout rather than HTTP; remove them, or run without --stdio", strings.Join(keys, ", "))
	}
	return nil
}

// validateHTTP validates the sections that configure HTTP.
func (c *Config) validateHTTP() error {
	var errs []error
	if err := validateListen(c.Listen, c.Auth != nil); err != nil {
		errs = append(errs, err)
	}
	if err := validateCertificate("listen_tls", c.ListenTLS); err != nil {
		errs = append(errs, err)
	}
	if c.Auth != nil {
		errs = append(errs, c.Auth.validate()...)
	}
	switch {
	case c.Auth != nil && c.Authorization == nil:
		errs = append(errs, errors.New("authorization: required with an auth section"))
	case c.Auth == nil && c.Authorization != nil:
		errs = append(errs, errors.New("authorization: not allowed without an auth section, whose access tokens carry the claims that rules match"))
	case c.Authorization != nil:
		errs = append(errs, c.Authorization.validate(c)...)
	}
	return errors.Join(errs...)
}

// validateTargets validates the targets and the codecs they use.
func (c *Config) validateTargets() error {
	var errs []error
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
		if err := validateSecret(label+": api_key", t.APIKey); err != nil {
			errs = append(errs, err)
		}
		if err := validateCertificate(label+": tls", t.TLS); err != nil {
			errs = append(errs, err)
		}
		if t.APIKey != nil && t.TLS != nil {
			errs = append(errs, fmt.Errorf("%s: at most one of api_key or tls is allowed", label))
		}
		switch t.PayloadHandling {
		case "", PayloadOmit, PayloadPassthrough, PayloadDecode:
		default:
			errs = append(errs, fmt.Errorf("%s: payload_handling: %q is not one of %s, %s or %s", label, t.PayloadHandling, PayloadOmit, PayloadPassthrough, PayloadDecode))
		}
		if _, ok := c.Codecs[t.Codec]; t.Codec != "" && !ok {
			errs = append(errs, fmt.Errorf("%s: codec %q is not one of codecs", label, t.Codec))
		}
		if t.PayloadHandling == PayloadDecode && t.Codec == "" {
			errs = append(errs, fmt.Errorf("%s: codec is required with payload_handling %s", label, PayloadDecode))
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.Codecs)) {
		codec, label := c.Codecs[name], fmt.Sprintf("codecs: %q", name)
		if err := validateURL(label+": endpoint", codec.Endpoint, true, codec.AllowPlainHTTP); err != nil {
			errs = append(errs, err)
		}
		if err := validateSecret(label+": authorization_header", codec.AuthorizationHeader); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *Authorization) validate(c *Config) []error {
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(a.Profiles)) {
		p, label := a.Profiles[name], fmt.Sprintf("authorization: profiles: %q", name)
		if len(p.Tools) == 0 {
			errs = append(errs, fmt.Errorf("%s: tools: at least one tool is required", label))
		}
		if len(p.Targets) == 0 {
			errs = append(errs, fmt.Errorf("%s: targets: at least one target is required", label))
		}
		for k, target := range p.Targets {
			if !slices.ContainsFunc(c.Targets, func(t Target) bool { return t.Name == target }) {
				errs = append(errs, fmt.Errorf("%s: targets[%d]: unknown target %q; profiles name targets by name, not alias", label, k, target))
			}
		}
	}
	if len(a.Rules) == 0 {
		errs = append(errs, errors.New("authorization: rules: at least one rule is required"))
	}
	for i, r := range a.Rules {
		label := fmt.Sprintf("authorization: rules[%d]", i)
		if strings.TrimSpace(r.ClaimName) == "" {
			errs = append(errs, fmt.Errorf("%s: claim_name is required", label))
		} else if r.ClaimName != strings.TrimSpace(r.ClaimName) {
			errs = append(errs, fmt.Errorf("%s: claim_name %q has leading or trailing whitespace, which no claim name matches", label, r.ClaimName))
		}
		if r.ClaimValue == "" {
			errs = append(errs, fmt.Errorf("%s: claim_value is required", label))
		} else if (r.ClaimName == "scope" || r.ClaimName == "scp") && strings.Contains(r.ClaimValue, " ") {
			errs = append(errs, fmt.Errorf("%s: claim_value %q contains a space, which no %s entry does; write one scope per rule", label, r.ClaimValue, r.ClaimName))
		}
		if r.Issuer != "" && !slices.ContainsFunc(c.Auth.Issuers, func(iss Issuer) bool { return iss.Issuer == r.Issuer }) {
			errs = append(errs, fmt.Errorf("%s: issuer %q is not one of auth: issuers", label, r.Issuer))
		}
		if r.Profile == "" {
			errs = append(errs, fmt.Errorf("%s: profile is required", label))
		} else if _, ok := a.Profiles[r.Profile]; !ok {
			errs = append(errs, fmt.Errorf("%s: profile %q is not one of authorization: profiles", label, r.Profile))
		}
	}
	return errs
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

func validateListen(listen string, authenticated bool) error {
	if listen == "" {
		return errors.New("listen is required")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if authenticated {
		return nil
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !addr.IsLoopback() {
		return fmt.Errorf("listen: host %q must be a loopback IP address such as 127.0.0.1 or ::1, because the config has no auth section", host)
	}
	return nil
}

func (a *Auth) validate() []error {
	var errs []error
	if err := validateURL("auth: resource", a.Resource, false, false); err != nil {
		errs = append(errs, err)
	}
	if len(a.Issuers) == 0 {
		errs = append(errs, errors.New("auth: issuers: at least one issuer is required"))
	}
	seen := map[string]int{}
	for i, iss := range a.Issuers {
		label := fmt.Sprintf("auth: issuers[%d]", i)
		if err := validateURL(label+": issuer", iss.Issuer, false, false); err != nil {
			errs = append(errs, err)
		} else if j, ok := seen[iss.Issuer]; ok {
			errs = append(errs, fmt.Errorf("%s: issuer %q conflicts with the issuer of issuers[%d]", label, iss.Issuer, j))
		} else {
			seen[iss.Issuer] = i
		}
		if strings.TrimSpace(iss.Audience) == "" {
			errs = append(errs, fmt.Errorf("%s: audience is required", label))
		}
	}
	for i, s := range a.AuthorizationServers {
		if err := validateURL(fmt.Sprintf("auth: authorization_servers[%d]", i), s, false, false); err != nil {
			errs = append(errs, err)
		}
	}
	for i, s := range a.ScopesSupported {
		if s == "" || strings.ContainsFunc(s, func(r rune) bool { return r <= ' ' || r == '"' || r == '\\' || r > '~' }) {
			errs = append(errs, fmt.Errorf(`auth: scopes_supported[%d]: %q is not a scope; scopes are non-empty and contain only printable ASCII characters other than space, " and \`, i, s))
		}
	}
	return errs
}

func validateCertificate(label string, c *Certificate) error {
	if c != nil && (c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("%s: cert_file and key_file are both required", label)
	}
	return nil
}

func validateSecret(label string, s *Secret) error {
	if s != nil && (s.Env == "") == (s.File == "") {
		return fmt.Errorf("%s: exactly one of env or file is required", label)
	}
	return nil
}

// validateURL requires value to be an https URL, or an http one for a loopback host or
// with allowPlainHTTP, with a host and no user info, query or fragment. A codec
// endpoint's {namespace} placeholder parses as a namespace.
func validateURL(label, value string, codecEndpoint, allowPlainHTTP bool) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	parsed := value
	if codecEndpoint {
		parsed = strings.ReplaceAll(value, "{namespace}", "namespace")
	}
	u, err := url.Parse(parsed)
	if err != nil || u.Scheme != "https" && (u.Scheme != "http" || !allowPlainHTTP && !LoopbackHost(u.Hostname())) || u.Host == "" || u.User != nil || strings.ContainsAny(value, "?#") {
		scheme := "an https URL (http only for a loopback host)"
		if codecEndpoint {
			scheme = "an https URL (http only for a loopback host, or for any host with allow_plain_http: true)"
		}
		return fmt.Errorf("%s: %q must be %s with a host and no user info, query or fragment", label, value, scheme)
	}
	return nil
}

// LoopbackHost reports whether host is localhost or a loopback IP address.
func LoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}
