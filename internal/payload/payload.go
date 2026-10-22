// Package payload renders the payloads in tool responses. Payloads are Temporal's
// customer application data, such as workflow inputs and results, memos, heartbeat
// details and failure details. Each target's payload_handling says how a response
// shows them: omitted, passed through as stored, or decoded by the target's codec
// server.
//
// A tool adds each payload of a response to one Batch, which returns the Field that
// stands for the payload in the response, and then renders the Batch once, which
// fills in every Field with at most one codec request.
package payload

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
)

// Field is a payload in a tool response. Batch.Render fills it in.
type Field struct {
	Handling  string  `json:"handling" jsonschema:"decoded, passthrough or omitted"`
	Value     any     `json:"value,omitempty" jsonschema:"with handling decoded or passthrough, the payload as JSON; absent if the payload is not JSON, and then encoding and data hold it"`
	Encoding  *string `json:"encoding,omitempty" jsonschema:"with handling omitted, or decoded or passthrough and no value: the payload's encoding, such as json/plain, binary/protobuf or binary/encrypted; an omitted payload's encoding is as stored, so json/plain or json/protobuf means passthrough would show it as JSON"`
	Data      *string `json:"data,omitempty" jsonschema:"with handling decoded or passthrough and no value: the payload's bytes in base64; passthrough bytes are as stored, so may be encrypted or compressed"`
	SizeBytes *int    `json:"size_bytes,omitempty" jsonschema:"with handling omitted: the size in bytes of the payload's data as stored, not counting its metadata"`
	Reason    string  `json:"reason,omitempty" jsonschema:"with handling omitted, why: payload_handling if this target leaves payloads out of responses, or decode_failed if the payload was not decoded"`
	Error     string  `json:"error,omitempty" jsonschema:"with reason decode_failed: why the payload was not decoded, such as that the codec server could not be reached or responded with an HTTP error"`
}

// Renderer renders the payloads of each target's tool responses.
type Renderer struct {
	targets map[string]*target
	logger  *slog.Logger
}

type target struct {
	name     string
	handling string
	codec    *codec // set when handling is decode
}

// NewRenderer returns the Renderer for cfg's targets. logger gets the cause of
// each payload that is not decoded. It fails if a codec's authorization header
// cannot be read.
func NewRenderer(cfg *config.Config, logger *slog.Logger) (*Renderer, error) {
	authorization := map[string]func() (string, error){}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(cfg.Codecs)) {
		if h := cfg.Codecs[name].AuthorizationHeader; h != nil {
			read, err := h.Reader()
			if err != nil {
				errs = append(errs, fmt.Errorf("codecs: %q: authorization_header: %w", name, err))
				continue
			}
			authorization[name] = read
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	r := &Renderer{targets: map[string]*target{}, logger: logger}
	for _, t := range cfg.Targets {
		rt := &target{name: t.Name, handling: t.PayloadHandling}
		if t.PayloadHandling == config.PayloadDecode {
			rt.codec = &codec{
				endpoint:      strings.ReplaceAll(cfg.Codecs[t.Codec].Endpoint, "{namespace}", t.Namespace),
				namespace:     t.Namespace,
				authorization: authorization[t.Codec],
			}
		}
		r.targets[t.Name] = rt
	}
	return r, nil
}

// CheckCodec sends the codec server of the named target, which must decode
// payloads, a decode request with no payloads, and returns why it failed, worded
// for agents, or "" if it succeeded. The cause of a failure goes to the log.
func (r *Renderer) CheckCodec(ctx context.Context, target string) string {
	if _, err := r.targets[target].codec.decode(ctx, nil); err != nil {
		r.logger.ErrorContext(ctx, "codec check failed", "target", target, "error", err.Error())
		return err.reason
	}
	return ""
}

// A Batch holds the payloads of one tool response, so that they are rendered together.
type Batch struct {
	r        *Renderer
	target   *target
	fields   []*Field
	payloads []*commonpb.Payload
}

// Batch returns an empty Batch for a response about the named target.
func (r *Renderer) Batch(target string) *Batch {
	return &Batch{r: r, target: r.targets[target]}
}

// Add adds p to b and returns the Field that stands for it, which stays empty until Render.
func (b *Batch) Add(p *commonpb.Payload) *Field {
	if p == nil {
		p = &commonpb.Payload{}
	}
	f := &Field{}
	b.fields = append(b.fields, f)
	b.payloads = append(b.payloads, p)
	return f
}

// AddAll adds each of ps to b and returns the Fields that stand for them, in
// order; the slice is empty but not nil when ps has no payloads.
func (b *Batch) AddAll(ps *commonpb.Payloads) []*Field {
	fields := []*Field{}
	for _, p := range ps.GetPayloads() {
		fields = append(fields, b.Add(p))
	}
	return fields
}

// Render fills in the Field of each payload in b as the target's payload_handling
// says. To decode, it sends every payload to the codec server in one request; each
// payload it cannot decode is omitted with reason decode_failed and an error that
// says why, and the cause goes to the log. Render returns an error only when ctx
// ends before the payloads are decoded.
func (b *Batch) Render(ctx context.Context) error {
	switch {
	case b.target != nil && b.target.handling == config.PayloadPassthrough:
		for i, p := range b.payloads {
			// A payload whose data is not the JSON its encoding says is shown as stored.
			*b.fields[i], _ = field("passthrough", p)
		}
	case b.target != nil && b.target.codec != nil:
		if len(b.payloads) > 0 {
			return b.decode(ctx)
		}
	default:
		b.omit("payload_handling", "")
	}
	return nil
}

func (b *Batch) decode(ctx context.Context) error {
	decoded, err := b.target.codec.decode(ctx, b.payloads)
	if err != nil {
		b.omit("decode_failed", err.reason)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b.r.logger.ErrorContext(ctx, "payloads not decoded", "target", b.target.name, "error", err.Error())
		return nil
	}
	for i, p := range decoded {
		f, ok := field("decoded", p)
		if !ok {
			// The encoding is json/plain or json/protobuf, so it holds no response content.
			why := fmt.Sprintf("the codec server decoded the payload to encoding %s, but its data is not valid UTF-8 JSON", encoding(p))
			f = omitted("decode_failed", why, b.payloads[i])
			b.r.logger.ErrorContext(ctx, "payload not decoded", "target", b.target.name, "error", why)
		}
		*b.fields[i] = f
	}
	return nil
}

func (b *Batch) omit(reason, why string) {
	for i, p := range b.payloads {
		*b.fields[i] = omitted(reason, why, p)
	}
}

// omitted renders p, which a response leaves out for reason, as its encoding and
// size as stored, without its data. why says why decoding failed, with reason decode_failed.
func omitted(reason, why string, p *commonpb.Payload) Field {
	enc, size := encoding(p), len(p.GetData())
	return Field{Handling: "omitted", Reason: reason, Error: why, Encoding: &enc, SizeBytes: &size}
}

// field renders p with the given handling: as JSON if its encoding is binary/null,
// or a JSON one and its data is valid UTF-8 JSON, and otherwise as its encoding and
// bytes. ok is false if p's encoding is a JSON one but its data is not valid UTF-8 JSON.
func field(handling string, p *commonpb.Payload) (f Field, ok bool) {
	switch encoding(p) {
	case converter.MetadataEncodingNil:
		return Field{Handling: handling, Value: json.RawMessage("null")}, true
	case converter.MetadataEncodingJSON, converter.MetadataEncodingProtoJSON:
		// Kept as raw JSON, so that numbers keep every digit.
		data := p.GetData()
		if !utf8.Valid(data) || !json.Valid(data) {
			return stored(handling, p), false
		}
		return Field{Handling: handling, Value: json.RawMessage(data)}, true
	}
	return stored(handling, p), true
}

func stored(handling string, p *commonpb.Payload) Field {
	enc, data := encoding(p), base64.StdEncoding.EncodeToString(p.GetData())
	return Field{Handling: handling, Encoding: &enc, Data: &data}
}

func encoding(p *commonpb.Payload) string {
	return string(p.GetMetadata()[converter.MetadataEncoding])
}
