// Package config loads and validates the server configuration file.
package config

import (
	"bytes"
	"cmp"
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
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	identitypb "go.temporal.io/cloud-sdk/api/identity/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.yaml.in/yaml/v3"
	"golang.org/x/net/http/httpguts"
)

// DefaultShutdownTimeout is the ShutdownTimeout of a config that does not set one.
const DefaultShutdownTimeout = 8 * time.Second

// Config is the server configuration.
type Config struct {
	// Listen is the host:port the HTTP server binds to. Without Auth, the host must be a loopback IP.
	Listen string `yaml:"listen"`
	// ListenTLS, when set, makes the server serve HTTPS on Listen rather than plain HTTP.
	ListenTLS *Certificate `yaml:"listen_tls"`
	// ShutdownTimeout is how long the server waits for the tool calls in progress
	// after SIGINT, SIGTERM or stdin closing before it cancels them. Zero means DefaultShutdownTimeout.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	Targets         []Target      `yaml:"targets"`
	// TemporalCloud, when set, has Load add targets for the namespaces of a Temporal Cloud account.
	TemporalCloud *TemporalCloud `yaml:"temporal_cloud"`
	// Codecs are the codec servers that targets name in their codec field, by name.
	Codecs map[string]Codec `yaml:"codecs"`
	// Auth, when set, requires every MCP request to carry a valid access token.
	Auth *Auth `yaml:"auth"`
	// Authorization decides which tools each caller may use on which targets. It is required with Auth, unless
	// TemporalCloud has Groups, and not allowed without Auth.
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

// Profile is permission to use its tools on each of Targets, which are target names.
// Its tools are Tools, or, if Tools is nil, every read-only tool except those in ToolsDeny.
type Profile struct {
	Tools     []string `yaml:"tools"`
	ToolsDeny []string `yaml:"tools_deny"`
	Targets   []string `yaml:"targets"`
}

// Rule gives Profile to callers whose access token has the top-level claim
// named ClaimName as a string equal to ClaimValue, or as an array with a string
// element equal to it. A string claim named scope or scp counts as an array of
// its space-separated entries.
type Rule struct {
	ClaimName  string `yaml:"claim_name"`
	ClaimValue string `yaml:"claim_value"`
	Profile    string `yaml:"profile"`
}

// Auth configures the server as an OAuth resource server that accepts JWT access tokens from one identity provider.
type Auth struct {
	// Resource is the URL at which clients reach this server's MCP endpoint.
	Resource string `yaml:"resource"`
	// Issuer is the exact iss claim of the identity provider's tokens.
	Issuer string `yaml:"issuer"`
	// Audience must identify this server's API, never the client app, whose ID tokens carry the client's aud.
	Audience string `yaml:"audience"`
	// RequiredScopes are the scopes every access token must have in its scope or scp claim.
	RequiredScopes []string `yaml:"required_scopes"`
	// ScopesSupported are the scopes advertised to clients; RequiredScopes if empty. They do not grant or deny access.
	ScopesSupported []string `yaml:"scopes_supported"`
	// AuthorizationServer is the authorization server advertised to clients; Issuer if empty.
	AuthorizationServer string `yaml:"authorization_server"`
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

// TemporalCloud is a Temporal Cloud account whose namespaces Load adds as
// targets: those that accept API keys and that Namespaces selects.
type TemporalCloud struct {
	// APIKey says where to read the API key that lists the account's namespaces
	// and user groups through the Cloud Ops API, and that each added target
	// connects with.
	APIKey     *Secret         `yaml:"api_key"`
	Namespaces CloudNamespaces `yaml:"namespaces"`
	// Groups, when set, gives the account's SCIM groups access through GroupAuthorization.
	Groups *CloudGroups `yaml:"groups"`
	// Added are the names of the targets that Load added for the account's namespaces.
	Added []string `yaml:"-"`
	// Report says how many namespaces Load listed, the target it added for each,
	// and why it added none for the others, one line each.
	Report []string `yaml:"-"`
	// namespaceIDs are the IDs of the namespaces that Load listed.
	namespaceIDs []string
}

// CloudNamespaces says which of a Temporal Cloud account's namespaces get a
// target, by their names without the account ID, and how those targets show
// payloads.
type CloudNamespaces struct {
	// Include are path.Match patterns, matched ignoring case; a namespace whose
	// name matches none of them gets no target. Nil means every namespace.
	Include []string `yaml:"include"`
	// Exclude are path.Match patterns, matched ignoring case; a namespace whose
	// name matches one of them gets no target.
	Exclude []string `yaml:"exclude"`
	// PayloadHandling is the PayloadHandling of each target, PayloadOmit or
	// PayloadPassthrough. Empty means PayloadOmit.
	PayloadHandling string `yaml:"payload_handling"`
}

// ListNamespacesFunc returns the namespaces that the Temporal Cloud Ops API lists for apiKey.
type ListNamespacesFunc func(apiKey string) ([]*namespacepb.Namespace, error)

// CloudGroups gives the members of a Temporal Cloud account's SCIM groups the
// targets of the account's namespaces that their groups can access.
type CloudGroups struct {
	// ClaimName is the access token claim that lists the caller's groups by
	// each SCIM group's idp_id.
	ClaimName string `yaml:"claim_name"`
	// ToolsDeny is the ToolsDeny of each group's profile.
	ToolsDeny []string `yaml:"tools_deny"`
}

// ListUserGroupsFunc returns the user groups that the Temporal Cloud Ops API lists for apiKey.
type ListUserGroupsFunc func(apiKey string) ([]*identitypb.UserGroup, error)

// groupProfilePrefix starts the name of each profile that GroupAuthorization
// adds, and of no profile in the authorization section.
const groupProfilePrefix = "cloud-group:"

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
	// PassthroughHeaders are the headers of a tool call's HTTP request that the
	// server copies onto that call's requests to the codec server.
	PassthroughHeaders []PassthroughHeader `yaml:"passthrough_headers"`
}

// PassthroughHeader is a header of a tool call's HTTP request that the server
// forwards to the codec server, with all its values.
type PassthroughHeader struct {
	// Name is the header's name on the tool call's request, compared ignoring case.
	Name string `yaml:"name"`
	// SendAs is the header's name on the codec request; Name if nil.
	SendAs *string `yaml:"send_as"`
}

// OutgoingName returns the name the header is sent under to the codec server.
func (h PassthroughHeader) OutgoingName() string {
	if h.SendAs != nil {
		return *h.SendAs
	}
	return h.Name
}

// outgoingField returns the config field that gives OutgoingName.
func (h PassthroughHeader) outgoingField() string {
	if h.SendAs != nil {
		return "send_as"
	}
	return "name"
}

// Secret says where to read a secret from. Exactly one field is set.
type Secret struct {
	// Env is the name of an environment variable holding the secret, read at startup and on every use.
	Env string `yaml:"env"`
	// File is the path of a file holding the secret, read at startup and on every use.
	File string `yaml:"file"`
}

// Read returns the secret, trimmed of surrounding whitespace. It reads the
// environment variable or file on every call, and fails if the variable is unset
// or empty or the file cannot be read or is empty.
func (s *Secret) Read() (string, error) {
	if s.Env != "" {
		v := strings.TrimSpace(os.Getenv(s.Env))
		if v == "" {
			return "", fmt.Errorf("environment variable %s is unset or empty", s.Env)
		}
		return v, nil
	}
	data, err := os.ReadFile(s.File)
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(string(data))
	if v == "" {
		return "", fmt.Errorf("%s is empty", s.File)
	}
	return v, nil
}

// Load reads the YAML file at path and returns the validated configuration for
// serving MCP over HTTP. If the file has a temporal_cloud section, Load first
// adds targets for the account's namespaces that list returns, and says what it
// did with each in TemporalCloud.Report, whose lines also end its error if the
// result is invalid.
func Load(path string, list ListNamespacesFunc) (*Config, error) {
	return load(path, func(data []byte) (*Config, error) { return parse(data, list) })
}

// LoadStdio reads the YAML file at path and returns the validated configuration
// for serving MCP over stdin and stdout, which must not have the sections that
// configure HTTP or a temporal_cloud section.
func LoadStdio(path string) (*Config, error) {
	return load(path, parseStdio)
}

// CheckSecrets reads every secret in c once: each target's api_key and each
// codec's authorization_header. It fails if an environment variable is unset or
// empty, or a file cannot be read or is empty, and reports every such secret.
// Load reads only temporal_cloud's api_key, and LoadStdio no secret, so the
// server calls CheckSecrets after loading its config.
func (c *Config) CheckSecrets() error {
	var errs []error
	for i, t := range c.Targets {
		if t.APIKey != nil {
			if _, err := t.APIKey.Read(); err != nil {
				errs = append(errs, fmt.Errorf("targets[%d]: api_key: %w", i, err))
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.Codecs)) {
		if h := c.Codecs[name].AuthorizationHeader; h != nil {
			if _, err := h.Read(); err != nil {
				errs = append(errs, fmt.Errorf("codecs: %q: authorization_header: %w", name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func load(path string, parse func([]byte) (*Config, error)) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parse(data)
}

func parse(data []byte, list ListNamespacesFunc) (*Config, error) {
	return decode(data, list, func(c *Config, _ *keyNodes) error { return errors.Join(c.validateHTTP(), nonStringRuleValues(data)) })
}

func parseStdio(data []byte) (*Config, error) {
	return decode(data, nil, func(_ *Config, k *keyNodes) error { return httpOnlyKeys(k) })
}

// keyNodes holds the YAML nodes of the keys whose presence matters, even with
// no value, except a profile's tools and tools_deny and temporal_cloud's
// namespaces include, groups and groups tools_deny, which decode reads from the
// authorization and temporal_cloud nodes. A missing key's node has Kind 0.
type keyNodes struct {
	Listen        yaml.Node `yaml:"listen"`
	ListenTLS     yaml.Node `yaml:"listen_tls"`
	Auth          yaml.Node `yaml:"auth"`
	Authorization yaml.Node `yaml:"authorization"`
	Targets       []struct {
		APIKey yaml.Node `yaml:"api_key"`
		TLS    yaml.Node `yaml:"tls"`
	} `yaml:"targets"`
	TemporalCloud yaml.Node `yaml:"temporal_cloud"`
	Codecs        map[string]struct {
		AuthorizationHeader yaml.Node `yaml:"authorization_header"`
		PassthroughHeaders  yaml.Node `yaml:"passthrough_headers"`
	} `yaml:"codecs"`
}

// decode decodes data, adds the targets of its temporal_cloud section if list
// is not nil, and validates the result with validate and validateTargets.
func decode(data []byte, list ListNamespacesFunc, validate func(*Config, *keyNodes) error) (*Config, error) {
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
	// A key with no value decodes to nil; validate it as empty rather than ignore it.
	var keys keyNodes
	if err := yaml.Unmarshal(data, &keys); err != nil {
		return nil, err
	}
	emptyIfPresent(&cfg.ListenTLS, keys.ListenTLS)
	emptyIfPresent(&cfg.Auth, keys.Auth)
	emptyIfPresent(&cfg.Authorization, keys.Authorization)
	emptyIfPresent(&cfg.TemporalCloud, keys.TemporalCloud)
	var authorization struct {
		Profiles map[string]struct {
			Tools     yaml.Node `yaml:"tools"`
			ToolsDeny yaml.Node `yaml:"tools_deny"`
		} `yaml:"profiles"`
	}
	if err := keys.Authorization.Decode(&authorization); err != nil {
		return nil, err
	}
	for name, k := range authorization.Profiles {
		p := cfg.Authorization.Profiles[name]
		emptyListIfPresent(&p.Tools, k.Tools)
		emptyListIfPresent(&p.ToolsDeny, k.ToolsDeny)
		cfg.Authorization.Profiles[name] = p
	}
	if cfg.TemporalCloud != nil {
		var temporalCloud struct {
			Namespaces struct {
				Include yaml.Node `yaml:"include"`
			} `yaml:"namespaces"`
			Groups yaml.Node `yaml:"groups"`
		}
		if err := keys.TemporalCloud.Decode(&temporalCloud); err != nil {
			return nil, err
		}
		emptyListIfPresent(&cfg.TemporalCloud.Namespaces.Include, temporalCloud.Namespaces.Include)
		emptyIfPresent(&cfg.TemporalCloud.Groups, temporalCloud.Groups)
		if cfg.TemporalCloud.Groups != nil {
			var groups struct {
				ToolsDeny yaml.Node `yaml:"tools_deny"`
			}
			if err := temporalCloud.Groups.Decode(&groups); err != nil {
				return nil, err
			}
			emptyListIfPresent(&cfg.TemporalCloud.Groups.ToolsDeny, groups.ToolsDeny)
		}
	}
	for i, t := range keys.Targets {
		emptyIfPresent(&cfg.Targets[i].APIKey, t.APIKey)
		emptyIfPresent(&cfg.Targets[i].TLS, t.TLS)
	}
	for name, c := range keys.Codecs {
		codec := cfg.Codecs[name]
		emptyIfPresent(&codec.AuthorizationHeader, c.AuthorizationHeader)
		emptyListIfPresent(&codec.PassthroughHeaders, c.PassthroughHeaders)
		var headers []struct {
			SendAs yaml.Node `yaml:"send_as"`
		}
		if err := c.PassthroughHeaders.Decode(&headers); err != nil {
			return nil, err
		}
		for i, h := range headers {
			emptyIfPresent(&codec.PassthroughHeaders[i].SendAs, h.SendAs)
		}
		cfg.Codecs[name] = codec
	}
	if cfg.TemporalCloud != nil && list != nil {
		if err := cfg.addCloudTargets(list); err != nil {
			return nil, err
		}
	}
	var shutdownErr error
	if cfg.ShutdownTimeout < 0 {
		shutdownErr = fmt.Errorf("shutdown_timeout: %s is negative", cfg.ShutdownTimeout)
	}
	if err := errors.Join(validate(&cfg, &keys), shutdownErr, cfg.validateTargets()); err != nil {
		if cfg.TemporalCloud != nil && len(cfg.TemporalCloud.Report) > 0 {
			errs := []error{err}
			for _, line := range cfg.TemporalCloud.Report {
				errs = append(errs, errors.New(line))
			}
			err = errors.Join(errs...)
		}
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

func emptyListIfPresent[T any](list *[]T, key yaml.Node) {
	if *list == nil && key.Kind != 0 {
		*list = []T{}
	}
}

// httpOnlyKeys rejects the sections that configure HTTP, temporal_cloud, and
// the codecs' passthrough_headers, which copy headers of HTTP requests; serving
// MCP over stdio supports none of them.
func httpOnlyKeys(k *keyNodes) error {
	var keys []string
	for _, key := range []struct {
		name string
		node yaml.Node
	}{{"listen", k.Listen}, {"listen_tls", k.ListenTLS}, {"auth", k.Auth}, {"authorization", k.Authorization}, {"temporal_cloud", k.TemporalCloud}} {
		if key.node.Kind != 0 {
			keys = append(keys, key.name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(k.Codecs)) {
		if k.Codecs[name].PassthroughHeaders.Kind != 0 {
			keys = append(keys, fmt.Sprintf("codecs: %q: passthrough_headers", name))
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
	groups := c.TemporalCloud != nil && c.TemporalCloud.Groups != nil
	switch {
	case c.Auth != nil && c.Authorization == nil && !groups:
		errs = append(errs, errors.New("authorization: required with an auth section, unless temporal_cloud has groups"))
	case c.Auth == nil && c.Authorization != nil:
		errs = append(errs, errors.New("authorization: not allowed without an auth section, whose access tokens carry the claims that rules match"))
	case c.Authorization != nil:
		errs = append(errs, c.Authorization.validate(c)...)
		for _, name := range slices.Sorted(maps.Keys(c.Authorization.Profiles)) {
			if strings.HasPrefix(name, groupProfilePrefix) {
				errs = append(errs, fmt.Errorf("authorization: profiles: %q: names starting with %s are reserved for the profiles of temporal_cloud groups", name, groupProfilePrefix))
			}
		}
		if len(c.Authorization.Rules) == 0 {
			errs = append(errs, errors.New("authorization: rules: at least one rule is required"))
		}
	}
	if groups && c.Auth == nil {
		errs = append(errs, errors.New("temporal_cloud: groups: not allowed without an auth section, whose access tokens carry the group names that groups match"))
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
		if strings.TrimSpace(t.Address) != "" {
			if err := validateAddress(t.Address); err != nil {
				errs = append(errs, fmt.Errorf("%s: address: %w", label, err))
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
		errs = append(errs, codec.validatePassthroughHeaders(label+": passthrough_headers")...)
	}
	return errors.Join(errs...)
}

// addCloudTargets adds the targets of the temporal_cloud section and records each listed namespace's outcome in TemporalCloud.Report.
func (c *Config) addCloudTargets(list ListNamespacesFunc) error {
	tc := c.TemporalCloud
	if err := tc.validate(); err != nil {
		return err
	}
	key, err := tc.APIKey.Read()
	if err != nil {
		return fmt.Errorf("temporal_cloud: api_key: %w", err)
	}
	namespaces, err := list(key)
	if err != nil {
		return fmt.Errorf("temporal_cloud: listing the account's namespaces: %w", err)
	}
	taken := func(name string) bool {
		_, ok := c.Lookup(name)
		return ok
	}
	tc.Report = append(tc.Report, fmt.Sprintf("temporal_cloud: namespaces listed by the API key: %d", len(namespaces)))
	for _, ns := range namespaces {
		id, name := ns.GetNamespace(), ns.GetSpec().GetName()
		tc.namespaceIDs = append(tc.namespaceIDs, id)
		var reason string
		switch i := slices.IndexFunc(c.Targets, func(t Target) bool { return t.Namespace == id }); {
		case i >= 0:
			reason = fmt.Sprintf("target %q has this namespace", c.Targets[i].Name)
		case !tc.Namespaces.selects(name):
			reason = "temporal_cloud's include and exclude patterns do not select it"
		case !ns.GetSpec().GetApiKeyAuth().GetEnabled() || ns.GetEndpoints().GetGrpcAddress() == "":
			reason = "it does not accept API keys"
		case taken(name) && taken(id):
			reason = "other targets already have its name and its namespace ID as names or aliases"
		}
		if reason != "" {
			tc.Report = append(tc.Report, fmt.Sprintf("temporal_cloud: namespace %s: added no target, because %s", id, reason))
			continue
		}
		if taken(name) {
			name = id
		}
		c.Targets = append(c.Targets, Target{
			Name:            name,
			Description:     cmp.Or(strings.TrimSpace(ns.GetSpec().GetDescription()), "Temporal Cloud namespace "+id),
			Address:         ns.GetEndpoints().GetGrpcAddress(),
			Namespace:       id,
			APIKey:          tc.APIKey,
			PayloadHandling: tc.Namespaces.PayloadHandling,
		})
		tc.Added = append(tc.Added, name)
		tc.Report = append(tc.Report, fmt.Sprintf("temporal_cloud: namespace %s: added target %q", id, name))
	}
	return nil
}

func (tc *TemporalCloud) validate() error {
	var errs []error
	if tc.APIKey == nil {
		errs = append(errs, errors.New("temporal_cloud: api_key is required"))
	} else if err := validateSecret("temporal_cloud: api_key", tc.APIKey); err != nil {
		errs = append(errs, err)
	}
	if tc.Namespaces.Include != nil && len(tc.Namespaces.Include) == 0 {
		errs = append(errs, errors.New("temporal_cloud: namespaces: include: at least one pattern is required; leave include out to select every namespace"))
	}
	for _, f := range []struct {
		name     string
		patterns []string
	}{{"include", tc.Namespaces.Include}, {"exclude", tc.Namespaces.Exclude}} {
		for i, p := range f.patterns {
			if _, err := path.Match(p, ""); err != nil {
				errs = append(errs, fmt.Errorf("temporal_cloud: namespaces: %s[%d]: %q is not a valid pattern", f.name, i, p))
			}
		}
	}
	switch tc.Namespaces.PayloadHandling {
	case "", PayloadOmit, PayloadPassthrough:
	default:
		errs = append(errs, fmt.Errorf("temporal_cloud: namespaces: payload_handling: %q is not one of %s or %s", tc.Namespaces.PayloadHandling, PayloadOmit, PayloadPassthrough))
	}
	if g := tc.Groups; g != nil {
		if err := validateClaimName("temporal_cloud: groups", g.ClaimName); err != nil {
			errs = append(errs, err)
		} else if g.ClaimName == "scope" || g.ClaimName == "scp" {
			errs = append(errs, fmt.Errorf("temporal_cloud: groups: claim_name %q is not allowed, because that claim holds the token's scopes, not group names", g.ClaimName))
		}
		if g.ToolsDeny != nil && len(g.ToolsDeny) == 0 {
			errs = append(errs, errors.New("temporal_cloud: groups: tools_deny: at least one tool is required"))
		}
	}
	return errors.Join(errs...)
}

// GroupAuthorization returns the authorization section, or an empty one, plus a
// profile and a rule for each SCIM group that list lists and that can read a
// target. It also returns a report: how many groups it listed, then one line
// per group in ID order with the targets the group gives and its namespaces
// that have no target, or why it gives none. It fails if it cannot list the
// groups or the result is invalid.
func (c *Config) GroupAuthorization(list ListUserGroupsFunc) (*Authorization, []string, error) {
	tc := c.TemporalCloud
	key, err := tc.APIKey.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("temporal_cloud: api_key: %w", err)
	}
	groups, err := list(key)
	if err != nil {
		return nil, nil, fmt.Errorf("temporal_cloud: listing the account's user groups: %w", err)
	}
	a := &Authorization{Profiles: map[string]Profile{}}
	if c.Authorization != nil {
		maps.Copy(a.Profiles, c.Authorization.Profiles)
		a.Rules = slices.Clone(c.Authorization.Rules)
	}
	allNamespaces := []identitypb.AccountAccess_Role{identitypb.AccountAccess_ROLE_OWNER, identitypb.AccountAccess_ROLE_ADMIN}
	reads := []identitypb.NamespaceAccess_Permission{identitypb.NamespaceAccess_PERMISSION_ADMIN, identitypb.NamespaceAccess_PERMISSION_WRITE, identitypb.NamespaceAccess_PERMISSION_READ}
	slices.SortFunc(groups, func(a, b *identitypb.UserGroup) int { return cmp.Compare(a.GetId(), b.GetId()) })
	report := []string{fmt.Sprintf("temporal_cloud: user groups listed by the API key: %d", len(groups))}
	for _, g := range groups {
		access := g.GetSpec().GetAccess()
		all := slices.Contains(allNamespaces, access.GetAccountAccess().GetRole())
		var targets, accessible, missing []string
		for _, t := range c.Targets {
			if slices.Contains(tc.namespaceIDs, t.Namespace) && (all || slices.Contains(reads, access.GetNamespaceAccesses()[t.Namespace].GetPermission())) {
				targets = append(targets, t.Name)
			}
		}
		for ns, na := range access.GetNamespaceAccesses() {
			if slices.Contains(reads, na.GetPermission()) {
				accessible = append(accessible, ns)
			}
		}
		slices.Sort(accessible)
		for _, ns := range accessible {
			if !slices.Contains(tc.namespaceIDs, ns) || !slices.ContainsFunc(c.Targets, func(t Target) bool { return t.Namespace == ns }) {
				missing = append(missing, ns)
			}
		}
		idpID, label := g.GetSpec().GetScimGroup().GetIdpId(), fmt.Sprintf("temporal_cloud: group %s %q", g.GetId(), g.GetSpec().GetDisplayName())
		switch {
		case idpID == "":
			report = append(report, label+": gives no targets, because it is not a SCIM group")
		case len(targets) > 0:
			name := groupProfilePrefix + g.GetId()
			a.Profiles[name] = Profile{ToolsDeny: tc.Groups.ToolsDeny, Targets: targets}
			a.Rules = append(a.Rules, Rule{ClaimName: tc.Groups.ClaimName, ClaimValue: idpID, Profile: name})
			line := fmt.Sprintf("%s: gives targets %q to callers whose %s claim has %q", label, targets, tc.Groups.ClaimName, idpID)
			if len(missing) > 0 {
				line += fmt.Sprintf("; its namespaces %q have no target", missing)
			}
			report = append(report, line)
		case all:
			report = append(report, label+": gives no targets, because none of the account's namespaces has a target")
		case len(accessible) == 0:
			report = append(report, label+": gives no targets, because it has no access to any namespace")
		default:
			report = append(report, fmt.Sprintf("%s: gives no targets, because its namespaces %q have no target", label, missing))
		}
	}
	if err := errors.Join(a.validate(c)...); err != nil {
		return nil, nil, err
	}
	return a, report, nil
}

// selects reports whether include and exclude select the namespace named name, without the account ID, ignoring case.
func (n *CloudNamespaces) selects(name string) bool {
	name = strings.ToLower(name)
	matches := func(patterns []string) bool {
		return slices.ContainsFunc(patterns, func(p string) bool {
			ok, _ := path.Match(strings.ToLower(p), name)
			return ok
		})
	}
	return (len(n.Include) == 0 || matches(n.Include)) && !matches(n.Exclude)
}

// reservedHeaders are the headers that the server or HTTP sets on a codec request
// itself, which no passthrough header may be named or sent as.
var reservedHeaders = []string{"Content-Type", "X-Namespace", "Host", "Content-Length", "Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "TE", "Trailer", "Upgrade"}

// headerNameProblem says why name cannot be a passthrough header's name or
// send_as, or returns "" if it can.
func headerNameProblem(name string) string {
	switch {
	case !httpguts.ValidHeaderFieldName(name):
		return "is not a valid HTTP header name"
	case slices.ContainsFunc(reservedHeaders, func(r string) bool { return strings.EqualFold(r, name) }):
		return "is not allowed, because the server or HTTP sets that header itself"
	}
	return ""
}

// validatePassthroughHeaders requires each header to have a name and an outgoing
// name that are valid, not reserved and not those of another header, a name other
// than Authorization, and an outgoing name other than Authorization when the codec
// has an authorization_header.
func (c *Codec) validatePassthroughHeaders(label string) []error {
	if c.PassthroughHeaders != nil && len(c.PassthroughHeaders) == 0 {
		return []error{fmt.Errorf("%s: at least one header is required", label)}
	}
	var errs []error
	for i, h := range c.PassthroughHeaders {
		label := fmt.Sprintf("%s[%d]", label, i)
		nameOK := false
		switch problem := headerNameProblem(h.Name); {
		case strings.TrimSpace(h.Name) == "":
			errs = append(errs, fmt.Errorf("%s: name is required", label))
		case problem != "":
			errs = append(errs, fmt.Errorf("%s: name %q %s", label, h.Name, problem))
		case strings.EqualFold(h.Name, "Authorization"):
			errs = append(errs, fmt.Errorf("%s: name %q is not allowed, because the server must not pass on the access token that MCP clients send in their Authorization header", label, h.Name))
		default:
			nameOK = true
		}
		outOK := nameOK
		if h.SendAs != nil {
			outOK = false
			switch problem := headerNameProblem(*h.SendAs); {
			case strings.TrimSpace(*h.SendAs) == "":
				errs = append(errs, fmt.Errorf("%s: send_as is empty; leave it out to send the header under its name", label))
			case problem != "":
				errs = append(errs, fmt.Errorf("%s: send_as %q %s", label, *h.SendAs, problem))
			case strings.EqualFold(*h.SendAs, "Authorization") && c.AuthorizationHeader != nil:
				errs = append(errs, fmt.Errorf("%s: send_as %q is not allowed with authorization_header, which sets that header", label, *h.SendAs))
			default:
				outOK = true
			}
		}
		for k, prev := range c.PassthroughHeaders[:i] {
			switch {
			case nameOK && strings.EqualFold(h.Name, prev.Name):
				errs = append(errs, fmt.Errorf("%s: name %q conflicts with name %q of passthrough_headers[%d]; header names are case-insensitive", label, h.Name, prev.Name, k))
			case outOK && strings.EqualFold(h.OutgoingName(), prev.OutgoingName()):
				errs = append(errs, fmt.Errorf("%s: %s %q conflicts with %s %q of passthrough_headers[%d]; header names are case-insensitive", label, h.outgoingField(), h.OutgoingName(), prev.outgoingField(), prev.OutgoingName(), k))
			}
		}
	}
	return errs
}

func (a *Authorization) validate(c *Config) []error {
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(a.Profiles)) {
		p, label := a.Profiles[name], fmt.Sprintf("authorization: profiles: %q", name)
		if p.Tools != nil && p.ToolsDeny != nil {
			errs = append(errs, fmt.Errorf("%s: at most one of tools or tools_deny is allowed", label))
		}
		for _, f := range []struct {
			name  string
			tools []string
		}{{"tools", p.Tools}, {"tools_deny", p.ToolsDeny}} {
			if f.tools != nil && len(f.tools) == 0 {
				errs = append(errs, fmt.Errorf("%s: %s: at least one tool is required", label, f.name))
			}
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
	for i, r := range a.Rules {
		label := fmt.Sprintf("authorization: rules[%d]", i)
		if err := validateClaimName(label, r.ClaimName); err != nil {
			errs = append(errs, err)
		}
		if r.ClaimValue == "" {
			errs = append(errs, fmt.Errorf("%s: claim_value is required", label))
		} else if (r.ClaimName == "scope" || r.ClaimName == "scp") && strings.Contains(r.ClaimValue, " ") {
			errs = append(errs, fmt.Errorf("%s: claim_value %q contains a space, which no %s entry does; write one scope per rule", label, r.ClaimValue, r.ClaimName))
		}
		if r.Profile == "" {
			errs = append(errs, fmt.Errorf("%s: profile is required", label))
		} else if _, ok := a.Profiles[r.Profile]; !ok {
			errs = append(errs, fmt.Errorf("%s: profile %q is not one of authorization: profiles", label, r.Profile))
		}
	}
	return errs
}

func validateClaimName(label, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s: claim_name is required", label)
	}
	if name != strings.TrimSpace(name) {
		return fmt.Errorf("%s: claim_name %q has leading or trailing whitespace, which no claim name matches", label, name)
	}
	return nil
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

// validateAddress requires address to be host:port, optionally after a dns:///
// prefix, with a host and a port from 1 to 65535.
func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(strings.TrimPrefix(address, "dns:///"))
	if err != nil {
		return fmt.Errorf("%w; it must be host:port, with a host and a port from 1 to 65535", err)
	}
	if host == "" || !validPort(port) {
		return fmt.Errorf("%q must be host:port, with a host and a port from 1 to 65535", address)
	}
	return nil
}

func validPort(s string) bool {
	n, err := strconv.ParseUint(s, 10, 16)
	return err == nil && n != 0
}

func (a *Auth) validate() []error {
	var errs []error
	if err := validateURL("auth: resource", a.Resource, false, false); err != nil {
		errs = append(errs, err)
	}
	if err := validateURL("auth: issuer", a.Issuer, false, false); err != nil {
		errs = append(errs, err)
	}
	if strings.TrimSpace(a.Audience) == "" {
		errs = append(errs, errors.New("auth: audience is required"))
	}
	if a.AuthorizationServer != "" {
		if err := validateURL("auth: authorization_server", a.AuthorizationServer, false, false); err != nil {
			errs = append(errs, err)
		}
	}
	if len(a.RequiredScopes) == 0 {
		errs = append(errs, errors.New("auth: required_scopes: at least one scope is required; use your API's scope as it appears in the scope or scp claim of a decoded access token"))
	}
	for _, f := range []struct {
		name   string
		scopes []string
	}{{"required_scopes", a.RequiredScopes}, {"scopes_supported", a.ScopesSupported}} {
		for i, s := range f.scopes {
			if s == "" || strings.ContainsFunc(s, func(r rune) bool { return r <= ' ' || r == '"' || r == '\\' || r > '~' }) {
				errs = append(errs, fmt.Errorf(`auth: %s[%d]: %q is not a scope; scopes are non-empty and contain only printable ASCII characters other than space, " and \`, f.name, i, s))
			}
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
