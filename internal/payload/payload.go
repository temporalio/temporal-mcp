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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	"github.com/temporalio/operator-mcp/internal/config"
)

// Field is a payload in a tool response. Batch.Render fills it in.
type Field struct {
	Handling string  `json:"handling" jsonschema:"decoded, passthrough or omitted"`
	Value    any     `json:"value,omitempty" jsonschema:"with handling decoded, the payload as JSON; absent if the payload is not JSON, and then encoding and data hold it"`
	Encoding *string `json:"encoding,omitempty" jsonschema:"with handling passthrough, or decoded without value: the payload's encoding, such as json/plain or binary/encrypted"`
	Data     *string `json:"data,omitempty" jsonschema:"with handling passthrough, or decoded without value: the payload's bytes in base64; passthrough bytes are as stored, so may be encrypted or compressed"`
	Reason   string  `json:"reason,omitempty" jsonschema:"with handling omitted, why: payload_handling if this target leaves payloads out of responses, decode_failed if its codec server could not decode this response's payloads, with the cause in the server log; check_connection's codec stage shows whether the codec server is reachable and accepts this server's credentials"`
}

// The Fields of payloads left out of a response.
var (
	omitted      = Field{Handling: "omitted", Reason: "payload_handling"}
	decodeFailed = Field{Handling: "omitted", Reason: "decode_failed"}
)

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
// is read from an environment variable that is unset or empty.
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
	_, err := r.targets[target].codec.decode(ctx, nil)
	ce := (*codecError)(nil)
	if !errors.As(err, &ce) {
		return ""
	}
	r.logger.ErrorContext(ctx, "codec check failed", "target", target, "error", err.Error())
	return ce.reason
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
// payload it cannot decode is omitted with reason decode_failed, and the cause goes
// to the log. Render returns an error only when ctx ends before the payloads are decoded.
func (b *Batch) Render(ctx context.Context) error {
	switch {
	case b.target != nil && b.target.handling == config.PayloadPassthrough:
		for i, p := range b.payloads {
			*b.fields[i] = stored("passthrough", p)
		}
	case b.target != nil && b.target.codec != nil:
		if len(b.payloads) > 0 {
			return b.decode(ctx)
		}
	default:
		b.fill(omitted)
	}
	return nil
}

func (b *Batch) decode(ctx context.Context) error {
	decoded, err := b.target.codec.decode(ctx, b.payloads)
	if err != nil {
		b.fill(decodeFailed)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b.r.logger.ErrorContext(ctx, "payloads not decoded", "target", b.target.name, "error", err.Error())
		return nil
	}
	for i, p := range decoded {
		f, err := decodedField(p)
		if err != nil {
			f = decodeFailed
			b.r.logger.ErrorContext(ctx, "payload not decoded", "target", b.target.name, "error", err.Error())
		}
		*b.fields[i] = f
	}
	return nil
}

func (b *Batch) fill(f Field) {
	for _, field := range b.fields {
		*field = f
	}
}

// decodedField renders a payload the codec server decoded: as JSON if its encoding
// is a JSON one, and otherwise as its encoding and bytes.
func decodedField(p *commonpb.Payload) (Field, error) {
	switch enc := string(p.GetMetadata()[converter.MetadataEncoding]); enc {
	case converter.MetadataEncodingNil:
		return Field{Handling: "decoded", Value: json.RawMessage("null")}, nil
	case converter.MetadataEncodingJSON, converter.MetadataEncodingProtoJSON:
		// Kept as raw JSON, so that numbers keep every digit.
		if !json.Valid(p.GetData()) {
			return Field{}, fmt.Errorf("the codec server decoded a payload to encoding %s, but its data is not JSON", enc)
		}
		return Field{Handling: "decoded", Value: json.RawMessage(p.GetData())}, nil
	}
	return stored("decoded", p), nil
}

func stored(handling string, p *commonpb.Payload) Field {
	enc, data := string(p.GetMetadata()[converter.MetadataEncoding]), base64.StdEncoding.EncodeToString(p.GetData())
	return Field{Handling: handling, Encoding: &enc, Data: &data}
}
