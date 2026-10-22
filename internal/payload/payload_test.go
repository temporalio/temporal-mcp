package payload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
)

func newRenderer(t *testing.T, endpoint string, auth *config.Secret, logs io.Writer) *Renderer {
	t.Helper()
	r, err := NewRenderer(&config.Config{
		Codecs:  map[string]config.Codec{"c": {Endpoint: endpoint, AuthorizationHeader: auth}},
		Targets: []config.Target{{Name: "t", Namespace: "payments.abc12", PayloadHandling: config.PayloadDecode, Codec: "c"}},
	}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func payload(encoding, data string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte(encoding)}, Data: []byte(data)}
}

func render(t *testing.T, ctx context.Context, b *Batch, ps ...*commonpb.Payload) (string, error) {
	t.Helper()
	fields := b.AddAll(&commonpb.Payloads{Payloads: ps})
	err := b.Render(ctx)
	out, jerr := json.Marshal(fields)
	if jerr != nil {
		t.Fatal(jerr)
	}
	return string(out), err
}

func TestRenderDecodes(t *testing.T) {
	type request struct {
		method, path string
		header       http.Header
		payloads     []*commonpb.Payload
	}
	var mu sync.Mutex
	var requests []request
	take := func() []request {
		mu.Lock()
		defer mu.Unlock()
		taken := requests
		requests = nil
		return taken
	}
	decoded := &commonpb.Payloads{Payloads: []*commonpb.Payload{
		payload("json/plain", `{"order_id": 9007199254740993}`),
		payload("binary/null", ""),
		payload("json/protobuf", `{"seconds":"5"}`),
		payload("binary/plain", "raw"),
		payload("json/plain", `{"truncated":`),
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in commonpb.Payloads
		if err := protojson.Unmarshal(body, &in); err != nil {
			t.Errorf("request body %s is not proto JSON payloads: %v", body, err)
		}
		mu.Lock()
		requests = append(requests, request{r.Method, r.URL.Path, r.Header, in.Payloads})
		mu.Unlock()
		b, _ := protojson.Marshal(decoded)
		w.Write(b)
	}))
	t.Cleanup(srv.Close)
	authFile := filepath.Join(t.TempDir(), "authorization")
	if err := os.WriteFile(authFile, []byte("Bearer one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	r := newRenderer(t, srv.URL+"/codecs/{namespace}", &config.Secret{File: authFile}, &logs)

	sent := []*commonpb.Payload{payload("binary/encrypted", "a"), payload("binary/encrypted", "b"), payload("binary/encrypted", "c"), payload("binary/encrypted", "d"), payload("binary/encrypted", "e")}
	for _, auth := range []string{"Bearer one", "Bearer two"} {
		if err := os.WriteFile(authFile, []byte(auth+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := render(t, t.Context(), r.Batch("t"), sent...)
		if err != nil {
			t.Fatal(err)
		}
		want := `[{"handling":"decoded","value":{"order_id":9007199254740993}},` +
			`{"handling":"decoded","value":null},` +
			`{"handling":"decoded","value":{"seconds":"5"}},` +
			`{"handling":"decoded","encoding":"binary/plain","data":"cmF3"},` +
			`{"handling":"omitted","encoding":"binary/encrypted","size_bytes":1,"reason":"decode_failed","error":"the codec server decoded the payload to encoding json/plain, but its data is not valid UTF-8 JSON"}]`
		if got != want {
			t.Errorf("fields = %s\nwant %s", got, want)
		}
		reqs := take()
		if len(reqs) != 1 {
			t.Fatalf("got %d codec requests for one batch, want 1", len(reqs))
		}
		req := reqs[0]
		if req.method != http.MethodPost || req.path != "/codecs/payments.abc12/decode" {
			t.Errorf("request = %s %s, want POST /codecs/payments.abc12/decode", req.method, req.path)
		}
		for name, want := range map[string]string{"Content-Type": "application/json", "X-Namespace": "payments.abc12", "Authorization": auth} {
			if got := req.header.Get(name); got != want {
				t.Errorf("header %s = %q, want %q", name, got, want)
			}
		}
		if !proto.Equal(&commonpb.Payloads{Payloads: req.payloads}, &commonpb.Payloads{Payloads: sent}) {
			t.Errorf("sent payloads %v, want %v", req.payloads, sent)
		}
	}
	if !strings.Contains(logs.String(), "the codec server decoded the payload to encoding json/plain, but its data is not valid UTF-8 JSON") {
		t.Errorf("log %q does not give the cause of the payload that is not JSON", logs.String())
	}

	if got, err := render(t, t.Context(), r.Batch("t")); err != nil || got != "[]" || len(take()) != 0 {
		t.Errorf("empty batch: fields %s, err %v; want [], no error and no codec request", got, err)
	}
}

func TestRenderPassesThrough(t *testing.T) {
	r, err := NewRenderer(&config.Config{
		Targets: []config.Target{{Name: "t", PayloadHandling: config.PayloadPassthrough}},
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	got, err := render(t, t.Context(), r.Batch("t"),
		payload("json/plain", `{"order_id": 9007199254740993}`),
		payload("binary/null", ""),
		payload("json/protobuf", `{"seconds":"5"}`),
		payload("binary/plain", "raw"),
		payload("json/plain", `{"truncated":`),
		payload("binary/encrypted", "secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"handling":"passthrough","value":{"order_id":9007199254740993}},` +
		`{"handling":"passthrough","value":null},` +
		`{"handling":"passthrough","value":{"seconds":"5"}},` +
		`{"handling":"passthrough","encoding":"binary/plain","data":"cmF3"},` +
		`{"handling":"passthrough","encoding":"json/plain","data":"eyJ0cnVuY2F0ZWQiOg=="},` +
		`{"handling":"passthrough","encoding":"binary/encrypted","data":"c2VjcmV0"}]`
	if got != want {
		t.Errorf("fields = %s\nwant %s", got, want)
	}
}

func TestRenderOmits(t *testing.T) {
	r, err := NewRenderer(&config.Config{
		Targets: []config.Target{{Name: "t", PayloadHandling: config.PayloadOmit}},
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	got, err := render(t, t.Context(), r.Batch("t"),
		payload("json/plain", `{"order_id": 9007199254740993}`),
		payload("binary/null", ""),
		payload("binary/encrypted", "secret"),
		payload("json/plain", ""),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"handling":"omitted","encoding":"json/plain","size_bytes":30,"reason":"payload_handling"},` +
		`{"handling":"omitted","encoding":"binary/null","size_bytes":0,"reason":"payload_handling"},` +
		`{"handling":"omitted","encoding":"binary/encrypted","size_bytes":6,"reason":"payload_handling"},` +
		`{"handling":"omitted","encoding":"json/plain","size_bytes":0,"reason":"payload_handling"}]`
	if got != want {
		t.Errorf("fields = %s\nwant %s", got, want)
	}
}

func TestRenderJSONUTF8(t *testing.T) {
	// The codec returns the payloads unchanged, including their original bytes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	}))
	t.Cleanup(srv.Close)
	var logs bytes.Buffer
	r, err := NewRenderer(&config.Config{
		Codecs: map[string]config.Codec{"c": {Endpoint: srv.URL}},
		Targets: []config.Target{
			{Name: "passthrough", PayloadHandling: config.PayloadPassthrough},
			{Name: "decode", PayloadHandling: config.PayloadDecode, Codec: "c"},
		},
	}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, data, base64 string
	}{
		{"invalid byte", "\"\xff\"", "Iv8i"},
		{"truncated sequence", "\"\xc3\"", "IsMi"},
		{"valid Unicode", `"café 🌍"`, ""},
	}
	for _, handling := range []string{"passthrough", "decode"} {
		for _, encoding := range []string{"json/plain", "json/protobuf"} {
			for _, tt := range tests {
				t.Run(handling+"/"+encoding+"/"+tt.name, func(t *testing.T) {
					logs.Reset()
					got, err := render(t, t.Context(), r.Batch(handling), payload(encoding, tt.data))
					if err != nil {
						t.Fatal(err)
					}
					shownHandling := "passthrough"
					if handling == "decode" {
						shownHandling = "decoded"
					}
					want := `[{"handling":"` + shownHandling + `","value":` + tt.data + `}]`
					if tt.base64 != "" {
						want = `[{"handling":"passthrough","encoding":"` + encoding + `","data":"` + tt.base64 + `"}]`
						if handling == "decode" {
							want = fmt.Sprintf(`[{"handling":"omitted","encoding":%[1]q,"size_bytes":%[2]d,"reason":"decode_failed","error":"the codec server decoded the payload to encoding %[1]s, but its data is not valid UTF-8 JSON"}]`, encoding, len(tt.data))
						}
					}
					if got != want {
						t.Errorf("fields = %q\nwant %q", got, want)
					}
					if handling == "decode" && tt.base64 != "" {
						if !strings.Contains(logs.String(), "but its data is not valid UTF-8 JSON") {
							t.Errorf("log %q does not explain the invalid UTF-8", logs.String())
						}
					} else if logs.Len() != 0 {
						t.Errorf("unexpected log: %q", logs.String())
					}
				})
			}
		}
	}
}

func TestRenderFailsClosed(t *testing.T) {
	respond := func(status int, body string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	tlsServer := httptest.NewUnstartedServer(http.NotFoundHandler())
	tlsServer.Config.ErrorLog = log.New(io.Discard, "", 0)
	tlsServer.StartTLS()
	t.Cleanup(tlsServer.Close)
	// The server notices that the client went away only once it has read the request body.
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(hang.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + ln.Addr().String()
	ln.Close()

	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		io.WriteString(w, "{")
	}))
	t.Cleanup(cut.Close)

	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("the codec client followed the codec server's redirect")
	}))
	t.Cleanup(redirectTarget.Close)
	redirect := httptest.NewServer(http.RedirectHandler(redirectTarget.URL, http.StatusTemporaryRedirect))
	t.Cleanup(redirect.Close)

	const customerData = "customer data"
	malformed := respond(http.StatusOK, `{"payloads":[{"data":"`+customerData+`"}]}`)
	authFile := filepath.Join(t.TempDir(), "authorization")
	if err := os.WriteFile(authFile, []byte("Bearer a\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		endpoint    string
		removedAuth *config.Secret
		reason      string
		timeout     time.Duration
	}{
		{"unauthorized", respond(http.StatusUnauthorized, customerData), nil, "the codec server responded HTTP 401 Unauthorized", 0},
		{"bad gateway", respond(http.StatusBadGateway, customerData), nil, "the codec server responded HTTP 502 Bad Gateway", 0},
		{"redirect", redirect.URL, nil, "the codec server responded HTTP 307 Temporary Redirect; redirects are not followed, so the codec endpoint must be the URL the codec server redirects to, without /decode", 0},
		{"permanent redirect", respond(http.StatusPermanentRedirect, ""), nil, "the codec server responded HTTP 308 Permanent Redirect; redirects are not followed, so the codec endpoint must be the URL the codec server redirects to, without /decode", 0},
		{"not payloads", malformed, nil, "the codec server's response is not a list of payloads", 0},
		{"wrong number of payloads", respond(http.StatusOK, `{"payloads":[{}]}`), nil, "the codec server returned a different number of payloads than it was sent", 0},
		{"unreachable", closed, nil, "could not connect to the codec server: connection refused", 0},
		{"untrusted certificate", tlsServer.URL, nil, "the codec server's TLS certificate does not chain to a trusted CA", 0},
		{"no response", hang.URL, nil, "timed out waiting for the codec server", 100 * time.Millisecond},
		{"response cut short", cut.URL, nil, "the codec server's response could not be read", 0},
		{"unreadable authorization header", malformed, &config.Secret{File: authFile}, "the authorization header for the codec server could not be read", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.timeout > 0 {
				// Only the deliberately unresponsive server needs a short codec timeout.
				defer func(d time.Duration) { codecTimeout = d }(codecTimeout)
				codecTimeout = tt.timeout
			}
			var logs bytes.Buffer
			r := newRenderer(t, tt.endpoint, tt.removedAuth, &logs)
			if tt.removedAuth != nil {
				if err := os.Remove(tt.removedAuth.File); err != nil {
					t.Fatal(err)
				}
			}
			if got := r.CheckCodec(t.Context(), "t"); got != tt.reason {
				t.Errorf("CheckCodec = %q, want %q", got, tt.reason)
			}
			// Each payload the codec server should have decoded gives the reason check_connection does.
			got, err := render(t, t.Context(), r.Batch("t"), payload("binary/encrypted", "a"), payload("binary/null", ""))
			want := fmt.Sprintf(`[{"handling":"omitted","encoding":"binary/encrypted","size_bytes":1,"reason":"decode_failed","error":%[1]q},`+
				`{"handling":"omitted","encoding":"binary/null","size_bytes":0,"reason":"decode_failed","error":%[1]q}]`, tt.reason)
			if got != want || err != nil {
				t.Errorf("fields = %s, err %v; want %s and no error", got, err, want)
			}
			if u, _ := url.Parse(tt.endpoint); strings.Contains(got, u.Hostname()) {
				t.Errorf("fields %s name the codec server's host %s", got, u.Hostname())
			}
			if log := logs.String(); !strings.Contains(log, tt.reason) || strings.Contains(log, customerData) {
				t.Errorf("log %q, want the cause and never the codec server's response or the authorization header", log)
			}
		})
	}

	var logs bytes.Buffer
	r := newRenderer(t, malformed, nil, &logs)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	want := `[{"handling":"omitted","encoding":"binary/encrypted","size_bytes":1,"reason":"decode_failed","error":"the request to the codec server failed"},` +
		`{"handling":"omitted","encoding":"binary/null","size_bytes":0,"reason":"decode_failed","error":"the request to the codec server failed"}]`
	if got, err := render(t, ctx, r.Batch("t"), payload("binary/encrypted", "a"), payload("binary/null", "")); got != want || !errors.Is(err, context.Canceled) || logs.Len() != 0 {
		t.Errorf("caller gone: fields = %s, err %v, log %q; want %s, the context's error and no log", got, err, logs.String(), want)
	}
}
