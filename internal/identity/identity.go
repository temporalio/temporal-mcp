// Package identity makes the server an OAuth resource server: it verifies the
// access tokens of MCP requests, serves the protected resource metadata that
// tells clients where to get them, and carries the verified caller to the code
// that serves the requests.
package identity

import (
	"cmp"
	"context"
	"crypto"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"

	"github.com/temporalio/temporal-mcp/internal/config"
)

var signingAlgorithms = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512,
	jose.PS256, jose.PS384, jose.PS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.EdDSA,
}

var registeredClaims = []string{"iss", "sub", "aud", "exp", "nbf", "iat", "jti"}

const (
	maxResponseBytes = 1 << 20
	maxLoggedChars   = 512
	maxURLBytes      = 512
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

var (
	discoveryRetryInterval = 30 * time.Second
	keyRefreshInterval     = time.Minute
	// keyMaxAge bounds how long a signing key the issuer has removed, such as a
	// compromised one, is still trusted, while the issuer's keys can be fetched.
	keyMaxAge = 5 * time.Minute
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
	iss, _ := ti.Extra["iss"].(string)
	sub, _ := ti.Extra["sub"].(string)
	return &Caller{Issuer: iss, Subject: sub, Claims: ti.Extra}
}

// Scopes returns the scopes in the caller's claim named claim, such as scope or
// scp, read as a space-separated string or an array of strings.
func (c *Caller) Scopes(claim string) []string {
	switch v := c.Claims[claim].(type) {
	case string:
		return strings.Split(v, " ")
	case []any:
		var scopes []string
		for _, e := range v {
			if s, ok := e.(string); ok {
				scopes = append(scopes, s)
			}
		}
		return scopes
	}
	return nil
}

// MetadataHandler serves the protected resource metadata (RFC 9728) for a.
func MetadataHandler(a *config.Auth) http.Handler {
	return auth.ProtectedResourceMetadataHandler(&oauthex.ProtectedResourceMetadata{
		Resource:             a.Resource,
		AuthorizationServers: []string{cmp.Or(a.AuthorizationServer, a.Issuer)},
		ScopesSupported:      scopesSupported(a),
	})
}

func scopesSupported(a *config.Auth) []string {
	if len(a.ScopesSupported) > 0 {
		return a.ScopesSupported
	}
	return a.RequiredScopes
}

// RequireToken returns a handler that serves a request with next only if it
// carries a valid access token from a's issuer that has each of a's
// required scopes in its scope or scp claim. A missing or invalid token gets a
// 401, and a valid token without a required scope a 403. Both get a
// WWW-Authenticate challenge that points to the protected resource metadata
// and lists the advertised scopes, with error="invalid_token" for an invalid
// token and error="insufficient_scope" for a missing scope. A token that
// cannot be checked yet gets a 500 with no challenge: when the issuer's
// discovery or signing keys are unavailable, or when the token's key is
// unknown and the request skipped fetching the keys because they were last
// fetched less than a minute before it, since the key may be newer than that
// fetch. When the request skipped fetching, an expired token gets a 401
// instead, whether that last fetch succeeded or failed. Response bodies are
// fixed ("no bearer token", "invalid token", "insufficient scope", "cannot
// verify access tokens"); the reason for a rejected token or a 500 goes to
// logger.
func RequireToken(a *config.Auth, logger *slog.Logger, next http.Handler) http.Handler {
	verify := newVerifier(a, logger)
	opts := &auth.RequireBearerTokenOptions{ClockSkew: jwt.DefaultLeeway}
	header := fmt.Sprintf("Bearer resource_metadata=%q, scope=%q", metadataURL(a.Resource), strings.Join(scopesSupported(a), " "))
	unwrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := w.(*challenge)
		caller := CallerFromTokenInfo(auth.TokenInfoFromContext(r.Context()))
		scopes := slices.Concat(caller.Scopes("scope"), caller.Scopes("scp"))
		for _, s := range a.RequiredScopes {
			if !slices.Contains(scopes, s) {
				logger.WarnContext(r.Context(), "rejected access token", "issuer", caller.Issuer, "reason", fmt.Sprintf("token lacks required scope %q", s))
				c.header = header + `, error="insufficient_scope"`
				http.Error(c, "insufficient scope", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(c.ResponseWriter, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := &challenge{ResponseWriter: w, header: header}
		auth.RequireBearerToken(func(ctx context.Context, token string, r *http.Request) (*auth.TokenInfo, error) {
			c.header = header + `, error="invalid_token"`
			return verify(ctx, token, r)
		}, opts)(unwrapped).ServeHTTP(c, r)
	})
}

type challenge struct {
	http.ResponseWriter
	header string
}

func (w *challenge) WriteHeader(code int) {
	if code == http.StatusUnauthorized || code == http.StatusForbidden {
		w.Header().Set("WWW-Authenticate", w.header)
	}
	w.ResponseWriter.WriteHeader(code)
}

// MetadataPath returns the path of the protected resource metadata for
// resource, which the WWW-Authenticate challenge points to.
func MetadataPath(resource string) string {
	u, _ := url.Parse(resource)
	if u.Path == "/" {
		return "/.well-known/oauth-protected-resource"
	}
	return "/.well-known/oauth-protected-resource" + u.Path
}

func metadataURL(resource string) string {
	u, _ := url.Parse(resource)
	u.Path = MetadataPath(resource)
	return u.String()
}

var errCannotVerify = errors.New("cannot verify access tokens")

type unavailableError struct {
	error
	// rateLimited reports that the request did not fetch the signing keys
	// because a fetch started less than keyRefreshInterval before it.
	rateLimited bool
}

type call struct {
	done     chan struct{}
	started  time.Time
	finished time.Time
	err      error
}

func (i *issuer) startCall(f func() error) *call {
	c := &call{done: make(chan struct{}), started: time.Now()}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				i.logger.Error("fetch from issuer panicked", "issuer", i.url, "error", fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
				c.err = fmt.Errorf("panic: %v", r)
			}
			c.finished = time.Now()
			close(c.done)
		}()
		c.err = f()
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
	loopback     bool
	verifyConfig *oidc.Config
	logger       *slog.Logger
	mu           sync.Mutex
	discovery    *call
	jwksURL      string
	fetch        *call
	keys         []jose.JSONWebKey
	fetched      time.Time
}

func (i *issuer) discover(ctx context.Context) (string, error) {
	i.mu.Lock()
	if i.jwksURL != "" {
		defer i.mu.Unlock()
		return i.jwksURL, nil
	}
	c := i.discovery
	if c == nil || c.isDone() && time.Since(c.finished) >= discoveryRetryInterval {
		c = i.startCall(i.discoverJWKSURL)
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
	var meta struct {
		Issuer  string `json:"issuer"`
		JWKSURL string `json:"jwks_uri"`
	}
	if err := i.get(strings.TrimSuffix(i.url, "/")+"/.well-known/openid-configuration", &meta); err != nil {
		return fmt.Errorf("discovering issuer %s: %w", i.url, err)
	}
	if meta.Issuer != i.url {
		return fmt.Errorf("discovering issuer %s: discovery document names issuer %.*q", i.url, maxLoggedChars, meta.Issuer)
	}
	if meta.JWKSURL == "" {
		return fmt.Errorf("discovering issuer %s: discovery document has no jwks_uri", i.url)
	}
	if len(meta.JWKSURL) > maxURLBytes {
		return fmt.Errorf("discovering issuer %s: jwks_uri is longer than %d bytes", i.url, maxURLBytes)
	}
	if u, err := url.Parse(meta.JWKSURL); err != nil || !i.allowedURL(u) || u.Host == "" {
		return fmt.Errorf("discovering issuer %s: jwks_uri %q must be an https URL (http only for a loopback host, when the issuer is on one)", i.url, meta.JWKSURL)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksURL = meta.JWKSURL
	return nil
}

func (i *issuer) allowedURL(u *url.URL) bool {
	return u.Scheme == "https" || u.Scheme == "http" && i.loopback && config.LoopbackHost(u.Hostname())
}

func (i *issuer) get(u string, v any) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%.*s", maxLoggedChars, err)
		}
	}()
	client := *httpClient
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !i.allowedURL(req.URL) {
			return errors.New("redirect must go to an https URL (http only for a loopback host, when the issuer is on one)")
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	resp, err := client.Get(u)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("%s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxLoggedChars))
		return fmt.Errorf("%s: %s: %q", u, resp.Status, body)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("%s: response is larger than %d bytes", u, maxResponseBytes)
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	return nil
}

// signingKeys returns the keys that may have signed a token with header,
// fetching the issuer's keys when they are older than keyMaxAge or none match,
// at most once per keyRefreshInterval.
func (i *issuer) signingKeys(ctx context.Context, jwksURL string, header jose.Header) ([]crypto.PublicKey, error) {
	i.mu.Lock()
	keys, fetched := i.keys, i.fetched
	i.mu.Unlock()
	if time.Since(fetched) < keyMaxAge {
		if matching := matchingKeys(keys, header); len(matching) > 0 {
			return matching, nil
		}
	}
	i.mu.Lock()
	c := i.fetch
	rateLimited := c != nil && c.isDone()
	if c == nil || rateLimited && time.Since(c.started) >= keyRefreshInterval {
		c, rateLimited = i.startCall(func() error {
			err := i.fetchKeys(jwksURL)
			if err != nil {
				i.logger.Warn("cannot fetch signing keys", "issuer", i.url, "error", err)
			}
			return err
		}), false
		i.fetch = c
	}
	i.mu.Unlock()
	if err := c.wait(ctx); err != nil {
		return nil, unavailableError{error: err}
	}
	i.mu.Lock()
	keys = i.keys
	i.mu.Unlock()
	if matching := matchingKeys(keys, header); len(matching) > 0 {
		return matching, nil
	}
	switch {
	case c.err != nil:
		return nil, unavailableError{error: c.err, rateLimited: rateLimited}
	case rateLimited:
		// The key may be newer than the last fetch, so the token is rejected only if it has expired.
		return nil, unavailableError{error: fmt.Errorf("no signing key matches the token, and the keys were fetched less than %v ago", keyRefreshInterval), rateLimited: true}
	}
	return nil, errors.New("no signing key matches the token")
}

func (i *issuer) fetchKeys(jwksURL string) error {
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := i.get(jwksURL, &set); err != nil {
		return fmt.Errorf("fetching signing keys: %w", err)
	}
	var keys []jose.JSONWebKey
	for _, raw := range set.Keys {
		var k jose.JSONWebKey
		if err := k.UnmarshalJSON(raw); err != nil {
			var meta struct {
				KeyID string `json:"kid"`
			}
			json.Unmarshal(raw, &meta)
			i.logger.Warn("skipped signing key", "issuer", i.url, "kid", fmt.Sprintf("%.*s", maxLoggedChars, meta.KeyID), "error", fmt.Sprintf("%.*s", maxLoggedChars, err))
			continue
		}
		if !k.IsPublic() || k.Use == "enc" || k.Algorithm != "" && !slices.Contains(signingAlgorithms, jose.SignatureAlgorithm(k.Algorithm)) {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return fmt.Errorf("fetching signing keys: %s: no usable signing key", jwksURL)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys, i.fetched = keys, time.Now()
	return nil
}

// matchingKeys returns the keys with header's kid, or all keys if it has none,
// leaving out those whose alg differs from header's.
func matchingKeys(keys []jose.JSONWebKey, header jose.Header) []crypto.PublicKey {
	var matching []crypto.PublicKey
	for _, k := range keys {
		if header.KeyID != "" && k.KeyID != header.KeyID || k.Algorithm != "" && k.Algorithm != header.Algorithm {
			continue
		}
		matching = append(matching, k.Key)
	}
	return matching
}

func (i *issuer) verify(ctx context.Context, token string, jws *jose.JSONWebSignature) (*auth.TokenInfo, error) {
	jwksURL, err := i.discover(ctx)
	if err != nil {
		return nil, unavailableError{error: err}
	}
	keys, err := i.signingKeys(ctx, jwksURL, jws.Signatures[0].Header)
	var unavailable unavailableError
	if errors.As(err, &unavailable) && unavailable.rateLimited && expired(jws.UnsafePayloadWithoutVerification()) {
		// A retry cannot make an expired token valid, so it gets the 401 that makes its client refresh it.
		return nil, errors.New("token is expired")
	}
	if err != nil {
		return nil, err
	}
	verified, err := oidc.NewVerifier(i.url, &oidc.StaticKeySet{PublicKeys: keys}, i.verifyConfig).Verify(ctx, token)
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

// expired reports whether payload's exp claim, which is unverified, is past the
// leeway.
func expired(payload []byte) bool {
	var claims jwt.Claims
	return json.Unmarshal(payload, &claims) == nil && errors.Is(claims.Validate(jwt.Expected{}), jwt.ErrExpired)
}

// parse returns the signed token if its alg is allowed, none of its claim names
// is duplicated or differs only in case from a registered one, and its iss is issuer.
func parse(token, issuer string) (*jose.JSONWebSignature, error) {
	jws, err := jose.ParseSignedCompact(token, signingAlgorithms)
	if errors.As(err, new(*jose.ErrUnexpectedSignatureAlgorithm)) {
		return nil, errors.New("unsupported signature algorithm")
	} else if err != nil {
		return nil, errors.New("malformed token")
	}
	var claims map[string]jsontext.Value
	if err := jsonv2.Unmarshal(jws.UnsafePayloadWithoutVerification(), &claims); errors.Is(err, jsontext.ErrDuplicateName) {
		return nil, errors.New("duplicate claim name")
	} else if err != nil || claims == nil {
		return nil, errors.New("malformed claims")
	}
	for name := range claims {
		for _, registered := range registeredClaims {
			if name != registered && strings.EqualFold(name, registered) {
				return nil, fmt.Errorf("claim name differs only in case from %q", registered)
			}
		}
	}
	raw, ok := claims["iss"]
	if !ok {
		return nil, errors.New("token has no iss claim")
	}
	var iss string
	if err := jsonv2.Unmarshal(raw, &iss); err != nil {
		return nil, errors.New("malformed claims")
	}
	if iss != issuer {
		return nil, fmt.Errorf("unknown issuer %.*q", maxLoggedChars, iss)
	}
	return jws, nil
}

func newVerifier(a *config.Auth, logger *slog.Logger) auth.TokenVerifier {
	algs := make([]string, len(signingAlgorithms))
	for k, alg := range signingAlgorithms {
		algs[k] = string(alg)
	}
	u, err := url.Parse(a.Issuer)
	iss := &issuer{url: a.Issuer, loopback: err == nil && config.LoopbackHost(u.Hostname()), verifyConfig: &oidc.Config{ClientID: a.Audience, SupportedSigningAlgs: algs, SkipExpiryCheck: true}, logger: logger}
	return func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		jws, err := parse(token, iss.url)
		if err != nil {
			// Logs at WARN like the rejections below, which any forged token with the configured iss already reaches.
			logger.WarnContext(ctx, "rejected access token", "reason", err)
			return nil, auth.ErrInvalidToken
		}
		info, err := iss.verify(ctx, token, jws)
		var unavailable unavailableError
		switch {
		case errors.As(err, &unavailable):
			level := slog.LevelError
			switch {
			case ctx.Err() != nil:
				level = slog.LevelDebug
			case unavailable.rateLimited:
				// Keeps tokens with made-up key IDs from flooding the log with errors.
				level = slog.LevelWarn
			}
			logger.Log(ctx, level, "cannot verify access token", "issuer", iss.url, "error", unavailable.error)
			return nil, errCannotVerify
		case err != nil:
			logger.WarnContext(ctx, "rejected access token", "issuer", iss.url, "reason", err)
			return nil, auth.ErrInvalidToken
		}
		return info, nil
	}
}
