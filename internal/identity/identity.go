// Package identity makes the server an OAuth resource server: it verifies the
// access tokens of MCP requests, serves the protected resource metadata that
// tells clients where to get them, and carries the verified caller to the code
// that serves the requests.
package identity

import (
	"context"
	"crypto"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"

	"github.com/temporalio/operator-mcp/internal/config"
)

var signingAlgorithms = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512,
	jose.PS256, jose.PS384, jose.PS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.EdDSA,
}

var registeredClaims = []string{"iss", "sub", "aud", "exp", "nbf", "iat", "jti"}

var httpClient = &http.Client{Timeout: 10 * time.Second}

var (
	discoveryRetryInterval = 30 * time.Second
	keyRefreshInterval     = time.Minute
)

// Caller is the verified identity behind a request, keyed by Issuer and Subject.
type Caller struct {
	Issuer  string
	Subject string
	// Claims are the verified claims of the caller's access token.
	Claims map[string]any
}

type callerKey struct{}

// WithCaller returns a context that carries caller.
func WithCaller(ctx context.Context, caller *Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// CallerFrom returns the caller that ctx carries, or nil if it carries none,
// as when auth is not configured.
func CallerFrom(ctx context.Context) *Caller {
	c, _ := ctx.Value(callerKey{}).(*Caller)
	return c
}

// CallerFromTokenInfo returns the caller for ti, which must come from RequireToken.
func CallerFromTokenInfo(ti *auth.TokenInfo) *Caller {
	iss, sub, _ := strings.Cut(ti.UserID, "#")
	return &Caller{Issuer: iss, Subject: sub, Claims: ti.Extra}
}

// MetadataHandler serves the protected resource metadata (RFC 9728) for a.
func MetadataHandler(a *config.Auth) http.Handler {
	servers := a.AuthorizationServers
	if len(servers) == 0 {
		for _, iss := range a.Issuers {
			servers = append(servers, iss.Issuer)
		}
	}
	return auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource:             a.Resource,
		AuthorizationServers: servers,
		ScopesSupported:      a.ScopesSupported,
	})
}

// RequireToken returns a handler that serves a request with next only if it
// carries a valid access token from one of a's issuers. A missing or invalid
// token gets a 401 with a WWW-Authenticate challenge that points to the
// protected resource metadata and lists a's scopes. If the issuer's discovery
// or signing keys are unavailable, the request gets a 500 with no challenge.
// Response bodies are fixed ("no bearer token", "invalid token", "cannot
// verify access tokens"); the reason for a rejected token or a 500 goes to
// logger.
func RequireToken(a *config.Auth, logger *slog.Logger, next http.Handler) http.Handler {
	require := auth.RequireBearerToken(newVerifier(a.Issuers, logger), &auth.RequireBearerTokenOptions{
		ResourceMetadataURL: metadataURL(a.Resource),
		ClockSkew:           jwt.DefaultLeeway,
	})
	if len(a.ScopesSupported) == 0 {
		return require(next)
	}
	scope := strings.Join(a.ScopesSupported, " ")
	inner := require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w.(scopeChallenge).ResponseWriter, r)
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.ServeHTTP(scopeChallenge{w, scope}, r)
	})
}

type scopeChallenge struct {
	http.ResponseWriter
	scope string
}

func (w scopeChallenge) WriteHeader(code int) {
	if code == http.StatusUnauthorized {
		h := w.Header()
		h.Set("WWW-Authenticate", fmt.Sprintf("%s, scope=%q", h.Get("WWW-Authenticate"), w.scope))
	}
	w.ResponseWriter.WriteHeader(code)
}

// MetadataPath returns the path of the protected resource metadata for
// resource, which the WWW-Authenticate challenge points to.
func MetadataPath(resource string) string {
	u, _ := url.Parse(resource)
	return "/.well-known/oauth-protected-resource" + strings.TrimSuffix(u.Path, "/")
}

func metadataURL(resource string) string {
	u, _ := url.Parse(resource)
	u.Path = MetadataPath(resource)
	return u.String()
}

var errCannotVerify = errors.New("cannot verify access tokens")

type unavailableError struct{ error }

type call struct {
	done     chan struct{}
	started  time.Time
	finished time.Time
	err      error
}

func startCall(f func() error) *call {
	c := &call{done: make(chan struct{}), started: time.Now()}
	go func() {
		c.err = f()
		c.finished = time.Now()
		close(c.done)
	}()
	return c
}

func (c *call) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *call) wait(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type issuer struct {
	url          string
	verifyConfig *oidc.Config
	logger       *slog.Logger
	mu           sync.Mutex
	discovery    *call
	jwksURL      string
	fetch        *call
	keys         []jose.JSONWebKey
}

func (i *issuer) discover(ctx context.Context) (string, error) {
	i.mu.Lock()
	if i.jwksURL != "" {
		defer i.mu.Unlock()
		return i.jwksURL, nil
	}
	c := i.discovery
	if c == nil || c.isDone() && time.Since(c.finished) >= discoveryRetryInterval {
		c = startCall(i.discoverJWKSURL)
		i.discovery = c
	}
	i.mu.Unlock()
	if err := c.wait(ctx); err != nil {
		return "", err
	}
	if c.err != nil {
		return "", c.err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.jwksURL, nil
}

func (i *issuer) discoverJWKSURL() error {
	p, err := oidc.NewProvider(oidc.ClientContext(context.Background(), httpClient), i.url)
	if err != nil {
		return fmt.Errorf("discovering issuer %s: %w", i.url, err)
	}
	var meta struct {
		JWKSURL string `json:"jwks_uri"`
	}
	if err := p.Claims(&meta); err != nil {
		return fmt.Errorf("discovering issuer %s: %w", i.url, err)
	}
	if meta.JWKSURL == "" {
		return fmt.Errorf("discovering issuer %s: no jwks_uri", i.url)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksURL = meta.JWKSURL
	return nil
}

func (i *issuer) signingKey(ctx context.Context, jwksURL string, jws *jose.JSONWebSignature) (crypto.PublicKey, error) {
	i.mu.Lock()
	keys := i.keys
	i.mu.Unlock()
	if key := verifyingKey(keys, jws); key != nil {
		return key, nil
	}
	i.mu.Lock()
	c := i.fetch
	if c == nil || c.isDone() && time.Since(c.started) >= keyRefreshInterval {
		c = startCall(func() error { return i.fetchKeys(jwksURL) })
		i.fetch = c
	}
	i.mu.Unlock()
	if err := c.wait(ctx); err != nil {
		return nil, unavailableError{err}
	}
	if c.err != nil {
		return nil, unavailableError{c.err}
	}
	i.mu.Lock()
	keys = i.keys
	i.mu.Unlock()
	if key := verifyingKey(keys, jws); key != nil {
		return key, nil
	}
	return nil, errors.New("no signing key verifies the token")
}

func (i *issuer) fetchKeys(jwksURL string) error {
	resp, err := httpClient.Get(jwksURL)
	if err != nil {
		return fmt.Errorf("fetching signing keys: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetching signing keys from %s: %s", jwksURL, resp.Status)
	}
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("fetching signing keys from %s: %w", jwksURL, err)
	}
	var keys []jose.JSONWebKey
	for _, raw := range set.Keys {
		var k jose.JSONWebKey
		if err := k.UnmarshalJSON(raw); err != nil {
			var meta struct {
				KeyID string `json:"kid"`
			}
			json.Unmarshal(raw, &meta)
			i.logger.Warn("skipped signing key", "issuer", i.url, "kid", meta.KeyID, "error", err)
			continue
		}
		if k.Algorithm != "" && !slices.Contains(signingAlgorithms, jose.SignatureAlgorithm(k.Algorithm)) {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return fmt.Errorf("fetching signing keys from %s: no usable signing key", jwksURL)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys = keys
	return nil
}

func verifyingKey(keys []jose.JSONWebKey, jws *jose.JSONWebSignature) crypto.PublicKey {
	kid := jws.Signatures[0].Header.KeyID
	for _, k := range keys {
		if kid != "" && k.KeyID != kid {
			continue
		}
		if _, err := jws.Verify(k.Key); err == nil {
			return k.Key
		}
	}
	return nil
}

func (i *issuer) verify(ctx context.Context, token string, jws *jose.JSONWebSignature) (*auth.TokenInfo, error) {
	jwksURL, err := i.discover(ctx)
	if err != nil {
		return nil, unavailableError{err}
	}
	key, err := i.signingKey(ctx, jwksURL, jws)
	if err != nil {
		return nil, err
	}
	verified, err := oidc.NewVerifier(i.url, &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{key}}, i.verifyConfig).Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	var registered jwt.Claims
	var claims map[string]any
	if err := verified.Claims(&registered); err != nil {
		return nil, err
	}
	if err := verified.Claims(&claims); err != nil {
		return nil, err
	}
	if registered.Expiry == nil {
		return nil, errors.New("token has no exp claim")
	}
	if registered.Subject == "" {
		return nil, errors.New("token has no sub claim")
	}
	if err := registered.Validate(jwt.Expected{}); err != nil {
		return nil, err
	}
	return &auth.TokenInfo{
		UserID:     registered.Issuer + "#" + registered.Subject,
		Expiration: registered.Expiry.Time(),
		Extra:      claims,
	}, nil
}

func unverifiedIssuer(payload []byte) (string, error) {
	var claims map[string]jsontext.Value
	if err := jsonv2.Unmarshal(payload, &claims); errors.Is(err, jsontext.ErrDuplicateName) {
		return "", errors.New("duplicate claim name")
	} else if err != nil || claims == nil {
		return "", errors.New("malformed claims")
	}
	for name := range claims {
		for _, registered := range registeredClaims {
			if name != registered && strings.EqualFold(name, registered) {
				return "", fmt.Errorf("claim name differs only in case from %q", registered)
			}
		}
	}
	raw, ok := claims["iss"]
	if !ok {
		return "", errors.New("token has no iss claim")
	}
	var iss string
	if err := jsonv2.Unmarshal(raw, &iss); err != nil {
		return "", errors.New("malformed claims")
	}
	return iss, nil
}

func newVerifier(issuers []config.Issuer, logger *slog.Logger) auth.TokenVerifier {
	algs := make([]string, len(signingAlgorithms))
	for k, alg := range signingAlgorithms {
		algs[k] = string(alg)
	}
	byIssuer := map[string]*issuer{}
	for _, iss := range issuers {
		byIssuer[iss.Issuer] = &issuer{url: iss.Issuer, verifyConfig: &oidc.Config{ClientID: iss.Audience, SupportedSigningAlgs: algs, SkipExpiryCheck: true}, logger: logger}
	}
	verify := func(ctx context.Context, token string) (string, *auth.TokenInfo, error) {
		jws, err := jose.ParseSignedCompact(token, signingAlgorithms)
		if errors.As(err, new(*jose.ErrUnexpectedSignatureAlgorithm)) {
			return "", nil, errors.New("unsupported signature algorithm")
		} else if err != nil {
			return "", nil, errors.New("malformed token")
		}
		name, err := unverifiedIssuer(jws.UnsafePayloadWithoutVerification())
		if err != nil {
			return "", nil, err
		}
		iss, ok := byIssuer[name]
		if !ok {
			return "", nil, errors.New("unknown issuer")
		}
		info, err := iss.verify(ctx, token, jws)
		return iss.url, info, err
	}
	return func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		iss, info, err := verify(ctx, token)
		var attrs []any
		if iss != "" {
			attrs = append(attrs, "issuer", iss)
		}
		var unavailable unavailableError
		switch {
		case errors.As(err, &unavailable):
			level := slog.LevelError
			if ctx.Err() != nil {
				level = slog.LevelDebug
			}
			logger.Log(ctx, level, "cannot verify access token", append(attrs, "error", unavailable.error)...)
			return nil, errCannotVerify
		case err != nil:
			logger.Warn("rejected access token", append(attrs, "reason", err)...)
			return nil, auth.ErrInvalidToken
		}
		return info, nil
	}
}
