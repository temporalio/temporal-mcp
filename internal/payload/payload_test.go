package payload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/temporalio/operator-mcp/internal/config"
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
			`{"handling":"omitted","reason":"decode_failed"}]`
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
	if !strings.Contains(logs.String(), "but its data is not JSON") {
		t.Errorf("log %q does not give the cause of the payload that is not JSON", logs.String())
	}

	if got, err := render(t, t.Context(), r.Batch("t")); err != nil || got != "[]" || len(take()) != 0 {
		t.Errorf("empty batch: fields %s, err %v; want [], no error and no codec request", got, err)
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

	defer func(d time.Duration) { codecTimeout = d }(codecTimeout)
	codecTimeout = 100 * time.Millisecond
	const customerData = "customer data"
	malformed := respond(http.StatusOK, `{"payloads":[{"data":"`+customerData+`"}]}`)

	tests := []struct {
		name     string
		endpoint string
		auth     *config.Secret
		reason   string
	}{
		{"unauthorized", respond(http.StatusUnauthorized, customerData), nil, "the codec server responded HTTP 401 Unauthorized"},
		{"redirect", redirect.URL, nil, "the codec server responded HTTP 307 Temporary Redirect"},
		{"not payloads", malformed, nil, "the codec server's response is not a list of payloads"},
		{"wrong number of payloads", respond(http.StatusOK, `{"payloads":[{}]}`), nil, "the codec server returned a different number of payloads than it was sent"},
		{"unreachable", closed, nil, "the request to the codec server failed"},
		{"untrusted certificate", tlsServer.URL, nil, "the TLS certificate check with the codec server failed"},
		{"no response", hang.URL, nil, "timed out waiting for the codec server"},
		{"response cut short", cut.URL, nil, "the codec server's response could not be read"},
		{"unreadable authorization header", malformed, &config.Secret{File: filepath.Join(t.TempDir(), "missing")}, "the authorization header for the codec server could not be read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			if got := newRenderer(t, tt.endpoint, tt.auth, &logs).CheckCodec(t.Context(), "t"); got != tt.reason {
				t.Errorf("CheckCodec = %q, want %q", got, tt.reason)
			}
			if log := logs.String(); !strings.Contains(log, tt.reason) || strings.Contains(log, customerData) {
				t.Errorf("log %q, want the cause and never the codec server's response or the authorization header", log)
			}
		})
	}

	var logs bytes.Buffer
	r := newRenderer(t, malformed, nil, &logs)
	want := `[{"handling":"omitted","reason":"decode_failed"}]`
	if got, err := render(t, t.Context(), r.Batch("t"), payload("binary/encrypted", "a")); got != want || err != nil {
		t.Errorf("fields = %s, err %v; want %s and no error", got, err, want)
	}
	if log := logs.String(); !strings.Contains(log, "the codec server's response is not a list of payloads") || strings.Contains(log, customerData) {
		t.Errorf("log %q, want the cause and never the codec server's response", log)
	}
	logs.Reset()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := render(t, ctx, r.Batch("t"), payload("binary/encrypted", "a")); got != want || !errors.Is(err, context.Canceled) || logs.Len() != 0 {
		t.Errorf("caller gone: fields = %s, err %v, log %q; want %s, the context's error and no log", got, err, logs.String(), want)
	}
}

func TestNewRendererWarnsOfPlainHTTP(t *testing.T) {
	for endpoint, warns := range map[string]bool{
		"http://codec.example.com":             true,
		"http://{namespace}.codec.example.com": true,
		"http://localhost:8080":                false,
		"http://127.0.0.1:8080":                false,
		"https://codec.example.com":            false,
	} {
		var logs bytes.Buffer
		newRenderer(t, endpoint, nil, &logs)
		if log := logs.String(); strings.Contains(log, "level=WARN") != warns || warns && !strings.Contains(log, "codec=c") {
			t.Errorf("%s: log %q, want warning %v (naming codec c)", endpoint, log, warns)
		}
	}
}
