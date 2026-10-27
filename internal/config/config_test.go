package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const validTargets = `
targets:
  - name: prod
    aliases: [production]
    description: Production.
    address: prod.example.tmprl.cloud:7233
    namespace: prod.abc12
`

func TestLoadValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("listen: 127.0.0.1:8080\nshutdown_timeout: 25s\n"+validTargets+`
  - {name: staging, description: Staging., address: "staging:7233", namespace: staging, payload_handling: decode, codec: payments}
  - {name: dev, description: Dev., address: "dev:7233", namespace: dev, codec: payments}
codecs:
  payments:
    endpoint: http://{namespace}.codec.internal
    allow_plain_http: true
    authorization_header: {env: CODEC_AUTHORIZATION}
  local:
    endpoint: http://localhost:8081/{namespace}
  secure:
    endpoint: https://codec.example.com/{namespace}
    allow_plain_http: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		Listen:          "127.0.0.1:8080",
		ShutdownTimeout: 25 * time.Second,
		Targets: []Target{{
			Name:        "prod",
			Aliases:     []string{"production"},
			Description: "Production.",
			Address:     "prod.example.tmprl.cloud:7233",
			Namespace:   "prod.abc12",
		}, {
			Name:            "staging",
			Description:     "Staging.",
			Address:         "staging:7233",
			Namespace:       "staging",
			PayloadHandling: PayloadDecode,
			Codec:           "payments",
		}, {
			Name:        "dev",
			Description: "Dev.",
			Address:     "dev:7233",
			Namespace:   "dev",
			Codec:       "payments",
		}},
		Codecs: map[string]Codec{
			"payments": {
				Endpoint:            "http://{namespace}.codec.internal",
				AllowPlainHTTP:      true,
				AuthorizationHeader: &Secret{Env: "CODEC_AUTHORIZATION"},
			},
			"local":  {Endpoint: "http://localhost:8081/{namespace}"},
			"secure": {Endpoint: "https://codec.example.com/{namespace}", AllowPlainHTTP: true},
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load = %+v, want %+v", cfg, want)
	}
}

const validAuthorization = `
authorization:
  profiles:
    oncall: {tools: [get_workflows], targets: [prod]}
  rules:
    - {claim_name: "cognito:groups", claim_value: oncall, profile: oncall}
`

func TestParseLoopbackListen(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:8080", "127.0.0.2:8080", "[::1]:8080", "127.0.0.1:0"} {
		if _, err := parse([]byte("listen: \"" + listen + "\"\n" + validTargets)); err != nil {
			t.Errorf("listen %s: %v", listen, err)
		}
	}
}

func TestParseTargetAddress(t *testing.T) {
	for _, address := range []string{"localhost:1", "localhost:65535", "dns:///prod.example.tmprl.cloud:7233"} {
		data := "listen: 127.0.0.1:8080\ntargets:\n  - {name: a, description: x, address: \"" + address + "\", namespace: a}\n"
		if _, err := parse([]byte(data)); err != nil {
			t.Errorf("address %s: %v", address, err)
		}
	}
}

func TestParseAuth(t *testing.T) {
	for _, u := range []string{"https://mcp.example.com/mcp", "http://LOCALHOST:8180/", "http://[::1]:8080"} {
		data := "listen: 0.0.0.0:8080\n" + validTargets + "auth:\n  resource: \"" + u + "\"\n  issuers: [{issuer: \"" + u + "\", audience: a}]\n  authorization_servers: [\"" + u + "\"]\n  scopes_supported: [openid, \"api://operator/Temporal.Read\"]\n" + validAuthorization
		if _, err := parse([]byte(data)); err != nil {
			t.Errorf("%s: %v", u, err)
		}
	}
}

func TestParseStdio(t *testing.T) {
	const auth = "auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: a}]\n"
	for _, tt := range []struct {
		name, yaml string
		want       []string
	}{
		{"targets only", validTargets, nil},
		{"shutdown timeout", "shutdown_timeout: 500ms\n" + validTargets, nil},
		{"negative shutdown timeout", "shutdown_timeout: -1s\n" + validTargets, []string{"shutdown_timeout: -1s is negative"}},
		{"listen", "listen: 127.0.0.1:8080\n" + validTargets, []string{"listen: not allowed with --stdio"}},
		{"empty listen", "listen: \"\"\n" + validTargets, []string{"listen: not allowed with --stdio"}},
		{"listen_tls", "listen_tls: {cert_file: a.crt, key_file: a.key}\n" + validTargets, []string{"listen_tls: not allowed with --stdio"}},
		{"auth", auth + validTargets, []string{"auth: not allowed with --stdio"}},
		{"empty auth", "auth:\n" + validTargets, []string{"auth: not allowed with --stdio"}},
		{"authorization", validTargets + validAuthorization, []string{"authorization: not allowed with --stdio"}},
		{"codec authorization header with no value", validTargets + "codecs:\n  c:\n    endpoint: https://codec.example.com\n    authorization_header:\n", []string{`codecs: "c": authorization_header: exactly one of env or file is required`}},
		{"every key and a bad target", "listen: 127.0.0.1:8080\nlisten_tls:\ntargets:\n  - {name: a}\n" + auth + validAuthorization, []string{
			"listen, listen_tls, auth, authorization: not allowed with --stdio",
			"targets[0]: address is required",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseStdio([]byte(tt.yaml))
			if tt.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.HasPrefix(err.Error(), tt.want[0]) {
				t.Errorf("error %q does not start with %q", err, tt.want[0])
			}
			for _, want := range tt.want[1:] {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestParseNamesDifferingOnlyByCase(t *testing.T) {
	data := "listen: 127.0.0.1:8080\ntargets:\n" +
		"  - {name: Prod, description: x, address: a:7233, namespace: a}\n" +
		"  - {name: prod, description: x, address: b:7233, namespace: b}\n"
	if _, err := parse([]byte(data)); err != nil {
		t.Fatal(err)
	}
}

func TestParseAPIKey(t *testing.T) {
	data := "listen: 127.0.0.1:8080\ntargets:\n" +
		"  - {name: a, description: x, address: a:7233, namespace: a, api_key: {env: A_KEY}}\n" +
		"  - {name: b, description: x, address: b:7233, namespace: b, api_key: {file: /keys/b}}\n"
	cfg, err := parse([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []*Secret{cfg.Targets[0].APIKey, cfg.Targets[1].APIKey}, []*Secret{{Env: "A_KEY"}, {File: "/keys/b"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("api keys = %+v, want %+v", got, want)
	}
}

func TestSecretRead(t *testing.T) {
	t.Setenv("TEMPORAL_MCP_TEST_SECRET", " env-secret\n")
	path := filepath.Join(t.TempDir(), "secret")
	env, file := (&Secret{Env: "TEMPORAL_MCP_TEST_SECRET"}).Read, (&Secret{File: path}).Read
	if _, err := file(); err == nil {
		t.Error("a missing file: want an error")
	}
	for _, tt := range []struct {
		read        func() (string, error)
		write, want string
	}{
		{env, "", "env-secret"},
		{file, "file-secret-1\n", "file-secret-1"},
		{file, "file-secret-2\n", "file-secret-2"},
	} {
		if tt.write != "" {
			if err := os.WriteFile(path, []byte(tt.write), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := tt.read(); err != nil || got != tt.want {
			t.Errorf("secret = %q, %v; want %q", got, err, tt.want)
		}
	}
	t.Setenv("TEMPORAL_MCP_TEST_SECRET", "changed")
	if got, err := env(); err != nil || got != "changed" {
		t.Errorf("secret after the environment variable changed = %q, %v; want %q", got, err, "changed")
	}
	if err := os.WriteFile(path, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := file(); err == nil {
		t.Error("an empty file: want an error")
	}
	t.Setenv("TEMPORAL_MCP_TEST_SECRET", " \n")
	if _, err := env(); err == nil {
		t.Error("an empty environment variable: want an error")
	}
}

func TestCheckSecrets(t *testing.T) {
	dir := t.TempDir()
	missing, empty, valid := filepath.Join(dir, "missing"), filepath.Join(dir, "empty"), filepath.Join(dir, "valid")
	for path, data := range map[string]string{empty: " \n", valid: "secret\n"} {
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TEMPORAL_MCP_TEST_SECRET", "secret")
	t.Setenv("TEMPORAL_MCP_TEST_UNSET_SECRET", "")
	os.Unsetenv("TEMPORAL_MCP_TEST_UNSET_SECRET")
	cfg := &Config{
		Targets: []Target{
			{Name: "a", APIKey: &Secret{File: missing}},
			{Name: "b", APIKey: &Secret{Env: "TEMPORAL_MCP_TEST_UNSET_SECRET"}},
			{Name: "c", APIKey: &Secret{File: valid}},
			{Name: "d", APIKey: &Secret{Env: "TEMPORAL_MCP_TEST_SECRET"}},
			{Name: "e"},
		},
		Codecs: map[string]Codec{
			"empty": {AuthorizationHeader: &Secret{File: empty}},
			"none":  {},
			"valid": {AuthorizationHeader: &Secret{File: valid}},
		},
	}
	want := "targets[0]: api_key: open " + missing + ": no such file or directory\n" +
		"targets[1]: api_key: environment variable TEMPORAL_MCP_TEST_UNSET_SECRET is unset or empty\n" +
		`codecs: "empty": authorization_header: ` + empty + " is empty"
	if err := cfg.CheckSecrets(); err == nil || err.Error() != want {
		t.Errorf("CheckSecrets = %v, want %q", err, want)
	}

	cfg.Targets = cfg.Targets[2:]
	delete(cfg.Codecs, "empty")
	if err := cfg.CheckSecrets(); err != nil {
		t.Errorf("CheckSecrets of readable secrets = %v, want no error", err)
	}
}

func TestLookup(t *testing.T) {
	cfg := &Config{Targets: []Target{
		{Name: "Prod", Aliases: []string{"production"}},
		{Name: "prod"},
	}}
	tests := []struct {
		name string
		want string
	}{
		{"Prod", "Prod"},
		{"prod", "prod"},
		{"production", "Prod"},
		{"PRODUCTION", "Prod"},
		{"PROD", ""},
		{"staging", ""},
	}
	for _, tt := range tests {
		got, ok := cfg.Lookup(tt.name)
		if tt.want == "" {
			if ok {
				t.Errorf("Lookup(%q) = %q, want no target", tt.name, got.Name)
			}
			continue
		}
		if !ok || got.Name != tt.want {
			t.Errorf("Lookup(%q) = %v, %v, want %q", tt.name, got, ok, tt.want)
		}
	}
}

func TestParseInvalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want []string
	}{
		{
			name: "empty",
			yaml: "",
			want: []string{"config is empty"},
		},
		{
			name: "more than one document",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "---\nlisten: 0.0.0.0:8080\n",
			want: []string{"config must contain exactly one YAML document"},
		},
		{
			name: "unknown top-level field",
			yaml: "listen: 127.0.0.1:8080\nport: 8080\n" + validTargets,
			want: []string{"field port not found"},
		},
		{
			name: "unknown target field",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - name: a\n    address: a:7233\n    namespace: a\n    bogus: true\n",
			want: []string{"field bogus not found"},
		},
		{
			name: "missing listen",
			yaml: validTargets,
			want: []string{"listen is required"},
		},
		{
			name: "negative shutdown timeout",
			yaml: "listen: 127.0.0.1:8080\nshutdown_timeout: -1s\n" + validTargets,
			want: []string{"shutdown_timeout: -1s is negative"},
		},
		{
			name: "shutdown timeout without a unit",
			yaml: "listen: 127.0.0.1:8080\nshutdown_timeout: 8\n" + validTargets,
			want: []string{"cannot unmarshal !!int `8` into time.Duration"},
		},
		{
			name: "listen on all interfaces",
			yaml: "listen: :8080\n" + validTargets,
			want: []string{`host "" must be a loopback IP address`},
		},
		{
			name: "listen on non-loopback IP",
			yaml: "listen: 0.0.0.0:8080\n" + validTargets,
			want: []string{`host "0.0.0.0" must be a loopback IP address`},
		},
		{
			name: "listen on hostname",
			yaml: "listen: localhost:8080\n" + validTargets,
			want: []string{`host "localhost" must be a loopback IP address`},
		},
		{
			name: "listen without port",
			yaml: "listen: 127.0.0.1\n" + validTargets,
			want: []string{"listen: address 127.0.0.1: missing port"},
		},
		{
			name: "listen_tls without key_file",
			yaml: "listen: 127.0.0.1:8080\nlisten_tls: {cert_file: /etc/temporal-mcp/tls.crt}\n" + validTargets,
			want: []string{"listen_tls: cert_file and key_file are both required"},
		},
		{
			name: "listen_tls with no value",
			yaml: "listen: 127.0.0.1:8080\nlisten_tls:\n" + validTargets,
			want: []string{"listen_tls: cert_file and key_file are both required"},
		},
		{
			name: "no targets key",
			yaml: "listen: 127.0.0.1:8080\n",
			want: []string{"at least one target is required"},
		},
		{
			name: "empty targets",
			yaml: "listen: 127.0.0.1:8080\ntargets: []\n",
			want: []string{"at least one target is required"},
		},
		{
			name: "missing required target fields",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - {}\n",
			want: []string{
				"targets[0]: name is required",
				"targets[0]: description is required",
				"targets[0]: address is required",
				"targets[0]: namespace is required",
			},
		},
		{
			name: "whitespace-only required target fields",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - {name: \" \", description: \"\\t\", address: \" \", namespace: \"  \"}\n",
			want: []string{"targets[0]: name is required", "targets[0]: description is required", "targets[0]: address is required", "targets[0]: namespace is required"},
		},
		{
			name: "address without a host or port",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: localhost, namespace: a}\n" +
				"  - {name: b, description: x, address: \":7233\", namespace: b}\n" +
				"  - {name: c, description: x, address: \"c:\", namespace: c}\n" +
				"  - {name: d, description: x, address: \"dns:///localhost\", namespace: d}\n",
			want: []string{
				`targets[0]: address: address localhost: missing port in address; it must be host:port, with a host and a port from 1 to 65535`,
				`targets[1]: address: ":7233" must be host:port, with a host and a port from 1 to 65535`,
				`targets[2]: address: "c:" must be host:port, with a host and a port from 1 to 65535`,
				`targets[3]: address: address localhost: missing port in address; it must be host:port, with a host and a port from 1 to 65535`,
			},
		},
		{
			name: "address with a port that is not a number from 1 to 65535, or a scheme other than dns:///",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: \"localhost:abc\", namespace: a}\n" +
				"  - {name: b, description: x, address: \"localhost:0\", namespace: b}\n" +
				"  - {name: c, description: x, address: \"localhost:99999\", namespace: c}\n" +
				"  - {name: d, description: x, address: \"https://host\", namespace: d}\n",
			want: []string{
				`targets[0]: address: "localhost:abc" must be host:port, with a host and a port from 1 to 65535`,
				`targets[1]: address: "localhost:0" must be host:port, with a host and a port from 1 to 65535`,
				`targets[2]: address: "localhost:99999" must be host:port, with a host and a port from 1 to 65535`,
				`targets[3]: address: "https://host" must be host:port, with a host and a port from 1 to 65535`,
			},
		},
		{
			name: "whitespace-only alias",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - name: a\n    aliases: [' ']\n    description: x\n    address: a:7233\n    namespace: a\n",
			want: []string{"targets[0]: aliases[0] is empty"},
		},
		{
			name: "empty alias",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - name: a\n    aliases: ['']\n    description: x\n    address: a:7233\n    namespace: a\n",
			want: []string{"targets[0]: aliases[0] is empty"},
		},
		{
			name: "duplicate names",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a}\n" +
				"  - {name: a, description: x, address: b:7233, namespace: b}\n",
			want: []string{`targets[1]: name "a" conflicts with the name of targets[0]`},
		},
		{
			name: "aliases differing only by case",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, aliases: [x], description: x, address: a:7233, namespace: a}\n" +
				"  - {name: b, aliases: [X], description: x, address: b:7233, namespace: b}\n",
			want: []string{`targets[1]: alias "X" conflicts with alias "x" of targets[0]; aliases are case-insensitive`},
		},
		{
			name: "duplicate aliases on one target",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, aliases: [x, x], description: x, address: a:7233, namespace: a}\n",
			want: []string{`targets[0]: alias "x" conflicts with alias "x" of this target; aliases are case-insensitive`},
		},
		{
			name: "alias matches another target's name ignoring case",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a}\n" +
				"  - {name: b, aliases: [A], description: x, address: b:7233, namespace: b}\n",
			want: []string{`targets[1]: alias "A" conflicts with name "a" of targets[0]; aliases are case-insensitive`},
		},
		{
			name: "alias matches a later target's name",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, aliases: [b], description: x, address: a:7233, namespace: a}\n" +
				"  - {name: b, description: x, address: b:7233, namespace: b}\n",
			want: []string{`targets[0]: alias "b" conflicts with name "b" of targets[1]; aliases are case-insensitive`},
		},
		{
			name: "alias repeats its own target's name",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, aliases: [a], description: x, address: a:7233, namespace: a}\n",
			want: []string{`targets[0]: alias "a" conflicts with name "a" of this target; aliases are case-insensitive`},
		},
		{
			name: "alias repeats its own target's name in another case",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: Prod, aliases: [prod], description: x, address: a:7233, namespace: a}\n",
			want: []string{`targets[0]: alias "prod" conflicts with name "Prod" of this target; aliases are case-insensitive`},
		},
		{
			name: "api key with env and file",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, api_key: {env: A_KEY, file: /keys/a}}\n",
			want: []string{"targets[0]: api_key: exactly one of env or file is required"},
		},
		{
			name: "api key without env or file",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, api_key: {}}\n",
			want: []string{"targets[0]: api_key: exactly one of env or file is required"},
		},
		{
			name: "api key with no value",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - name: a\n    description: x\n    address: a:7233\n    namespace: a\n    api_key:\n",
			want: []string{"targets[0]: api_key: exactly one of env or file is required"},
		},
		{
			name: "tls with no value",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n  - name: a\n    description: x\n    address: a:7233\n    namespace: a\n    tls:\n",
			want: []string{"targets[0]: tls: cert_file and key_file are both required"},
		},
		{
			name: "tls without key_file",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, tls: {cert_file: /certs/a.crt}}\n",
			want: []string{"targets[0]: tls: cert_file and key_file are both required"},
		},
		{
			name: "api key and tls",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, api_key: {env: A_KEY}, tls: {cert_file: /certs/a.crt, key_file: /certs/a.key}}\n",
			want: []string{"targets[0]: at most one of api_key or tls is allowed"},
		},
		{
			name: "inline api key",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, api_key: secret}\n",
			want: []string{"cannot unmarshal !!str `secret` into config.Secret"},
		},
		{
			name: "bad payload handling",
			yaml: "listen: 127.0.0.1:8080\ntargets:\n" +
				"  - {name: a, description: x, address: a:7233, namespace: a, payload_handling: Decode}\n" +
				"  - {name: b, description: x, address: b:7233, namespace: b, payload_handling: decode}\n" +
				"  - {name: c, description: x, address: c:7233, namespace: c, payload_handling: decode, codec: missing}\n" +
				"codecs:\n  c: {endpoint: https://codec.example.com}\n",
			want: []string{
				`targets[0]: payload_handling: "Decode" is not one of omit, passthrough or decode`,
				"targets[1]: codec is required with payload_handling decode",
				`targets[2]: codec "missing" is not one of codecs`,
			},
		},
		{
			name: "bad codecs",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + `codecs:
  none: {}
  grpc: {endpoint: "grpc://{namespace}.codec.example.com"}
  plain: {endpoint: "http://{namespace}.codec.example.com"}
  neither: {endpoint: https://codec.example.com, authorization_header: {}}
`,
			want: []string{
				`codecs: "none": endpoint is required`,
				`codecs: "grpc": endpoint: "grpc://{namespace}.codec.example.com" must be an https URL (http only for a loopback host, or for any host with allow_plain_http: true) with a host and no user info, query or fragment`,
				`codecs: "plain": endpoint: "http://{namespace}.codec.example.com" must be an https URL (http only for a loopback host, or for any host with allow_plain_http: true) with a host and no user info, query or fragment`,
				`codecs: "neither": authorization_header: exactly one of env or file is required`,
			},
		},
		{
			name: "codec authorization header with no value",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "codecs:\n  c:\n    endpoint: https://codec.example.com\n    authorization_header:\n",
			want: []string{`codecs: "c": authorization_header: exactly one of env or file is required`},
		},
		{
			name: "codec authorization header with its lines commented out",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "codecs:\n  c:\n    endpoint: https://codec.example.com\n    authorization_header:\n      # env: CODEC_AUTH\n      # file: /keys/codec\n",
			want: []string{`codecs: "c": authorization_header: exactly one of env or file is required`},
		},
		{
			name: "auth with no value",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "auth:\n",
			want: []string{"auth: resource is required", "auth: issuers: at least one issuer is required"},
		},
		{
			name: "bad auth URLs",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + `auth:
  resource: http://mcp.example.com/mcp
  issuers:
    - {issuer: "https://idp.example.com?tenant=1", audience: a}
    - {issuer: "https://user@idp.example.com", audience: a}
    - {issuer: "https://", audience: a}
    - {audience: a}
    - {issuer: "https://idp.example.com/#tenant", audience: a}
  authorization_servers: ["not a url"]
`,
			want: []string{
				`auth: resource: "http://mcp.example.com/mcp" must be an https URL (http only for a loopback host)`,
				`auth: issuers[0]: issuer: "https://idp.example.com?tenant=1" must be`,
				`auth: issuers[1]: issuer: "https://user@idp.example.com" must be`,
				`auth: issuers[2]: issuer: "https://" must be`,
				"auth: issuers[3]: issuer is required",
				`auth: issuers[4]: issuer: "https://idp.example.com/#tenant" must be`,
				`auth: authorization_servers[0]: "not a url" must be`,
			},
		},
		{
			name: "duplicate issuers and missing audiences",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + `auth:
  resource: https://mcp.example.com/mcp
  issuers:
    - {issuer: https://idp.example.com, audience: a}
    - {issuer: https://idp.example.com, audience: b}
    - {issuer: https://other.example.com, audience: " "}
`,
			want: []string{
				`auth: issuers[1]: issuer "https://idp.example.com" conflicts with the issuer of issuers[0]`,
				"auth: issuers[2]: audience is required",
			},
		},
		{
			name: "bad scopes",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: a}]\n  scopes_supported: ['', 'a b', 'a\"b', 'a\\b', 'é']\n",
			want: []string{
				`auth: scopes_supported[0]: "" is not a scope`,
				`auth: scopes_supported[1]: "a b" is not a scope`,
				`auth: scopes_supported[2]: "a\"b" is not a scope`,
				`auth: scopes_supported[3]: "a\\b" is not a scope`,
				`auth: scopes_supported[4]: "é" is not a scope`,
			},
		},
		{
			name: "auth without authorization",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: a}]\n",
			want: []string{"authorization: required with an auth section"},
		},
		{
			name: "authorization without auth",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + validAuthorization,
			want: []string{"authorization: not allowed without an auth section"},
		},
		{
			name: "authorization with no value and no auth",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "authorization:\n",
			want: []string{"authorization: not allowed without an auth section"},
		},
		{
			name: "authorization with no value",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: a}]\nauthorization:\n",
			want: []string{"authorization: rules: at least one rule is required"},
		},
		{
			name: "bad profiles and rules",
			yaml: "listen: 127.0.0.1:8080\n" + validTargets + "auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: a}]\n" + `authorization:
  profiles:
    empty: {}
    bad-targets: {tools: [get_workflows], targets: [production, staging]}
    both: {tools: [get_workflows], tools_deny: [count_workflows], targets: [prod]}
    blank-tools:
      tools:
      targets: [prod]
    empty-tools: {tools: [], targets: [prod]}
    blank-deny:
      tools_deny:
      targets: [prod]
    empty-deny: {tools_deny: [], targets: [prod]}
  rules:
    - {claim_name: groups, profile: empty}
    - {claim_name: " ", claim_value: a, profile: empty}
    - {claim_name: email, claim_value: a, issuer: https://other.example.com, profile: empty}
    - {claim_name: groups, claim_value: a, profile: missing}
    - {claim_name: groups, claim_value: a}
    - {claim_name: " groups ", claim_value: a, profile: empty}
    - {claim_name: groups, claim_value: true, profile: empty}
    - {claim_name: scp, claim_value: "openid temporal.orders", profile: empty}
`,
			want: []string{
				`authorization: profiles: "bad-targets": targets[0]: unknown target "production"; profiles name targets by name, not alias`,
				`authorization: profiles: "bad-targets": targets[1]: unknown target "staging"`,
				`authorization: profiles: "empty": targets: at least one target is required`,
				`authorization: profiles: "both": at most one of tools or tools_deny is allowed`,
				`authorization: profiles: "blank-tools": tools: at least one tool is required`,
				`authorization: profiles: "empty-tools": tools: at least one tool is required`,
				`authorization: profiles: "blank-deny": tools_deny: at least one tool is required`,
				`authorization: profiles: "empty-deny": tools_deny: at least one tool is required`,
				"authorization: rules[0]: claim_value is required",
				"authorization: rules[1]: claim_name is required",
				`authorization: rules[2]: issuer "https://other.example.com" is not one of auth: issuers`,
				`authorization: rules[3]: profile "missing" is not one of authorization: profiles`,
				"authorization: rules[4]: profile is required",
				`authorization: rules[5]: claim_name " groups " has leading or trailing whitespace, which no claim name matches`,
				`authorization: rules[6]: claim_value: true is not a string, and rules match only string claim values; quote it to match the string "true"`,
				`authorization: rules[7]: claim_value "openid temporal.orders" contains a space, which no scp entry does; write one scope per rule`,
			},
		},
		{
			name: "all errors reported",
			yaml: "listen: 0.0.0.0:8080\ntargets:\n  - {name: a}\n",
			want: []string{
				"must be a loopback IP address",
				"targets[0]: description is required",
				"targets[0]: address is required",
				"targets[0]: namespace is required",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parse([]byte(tt.yaml))
			if err == nil {
				t.Fatal("want an error")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}
