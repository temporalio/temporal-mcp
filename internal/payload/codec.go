// The codec server client, which speaks the protocol of the Temporal CLI's
// --codec-endpoint: a POST to the endpoint's /decode path, with the payloads as
// temporal.api.common.v1.Payloads in proto JSON, the target's namespace in the
// X-Namespace header, and the configured Authorization header.

package payload

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// codecTimeout bounds a codec request, so a hung codec server leaves payloads undecoded rather than timing out the call.
var codecTimeout = 10 * time.Second

// codecClient does not follow redirects, which could carry the authorization header to plain HTTP.
var codecClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// maxCodecResponse bounds the codec response read into memory, at the Temporal client's limit on a Temporal response.
const maxCodecResponse = 128 << 20

type codec struct {
	endpoint      string
	namespace     string
	authorization func() (string, error) // nil without an authorization header
}

// A codecError is a failed codec request. Its reason is worded for agents and names
// no address, path or response content. Its cause, for the log, may name an address
// or path but never response content; it is nil when the reason says it all.
type codecError struct {
	reason string
	cause  error
}

func (e *codecError) Error() string {
	if e.cause == nil {
		return e.reason
	}
	return e.reason + ": " + e.cause.Error()
}

// decode returns ps as the codec server decodes them, in order. Every error is a *codecError.
func (c *codec) decode(ctx context.Context, ps []*commonpb.Payload) ([]*commonpb.Payload, error) {
	// Proto JSON leaves out an empty list, which some codec servers then fail to read.
	body := []byte(`{"payloads":[]}`)
	if len(ps) > 0 {
		var err error
		if body, err = protojson.Marshal(&commonpb.Payloads{Payloads: ps}); err != nil {
			return nil, &codecError{"the payloads could not be encoded for the codec server", err}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, codecTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/decode", bytes.NewReader(body))
	if err != nil {
		return nil, &codecError{"the codec server's endpoint is not a valid URL", err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Namespace", c.namespace)
	if c.authorization != nil {
		v, err := c.authorization()
		if err != nil {
			return nil, &codecError{"the authorization header for the codec server could not be read", err}
		}
		req.Header.Set("Authorization", v)
	}
	resp, err := codecClient.Do(req)
	if err != nil {
		return nil, &codecError{requestFailure(err), err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason := fmt.Sprintf("the codec server responded HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		if resp.StatusCode/100 == 3 {
			reason += "; redirects are not followed, so the codec endpoint must be the URL the codec server redirects to, without /decode"
		}
		return nil, &codecError{reason, nil}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxCodecResponse+1))
	if err != nil {
		return nil, &codecError{"the codec server's response could not be read", err}
	}
	if len(data) > maxCodecResponse {
		return nil, &codecError{"the codec server's response is larger than 128 MiB", nil}
	}
	var out commonpb.Payloads
	// protojson's error quotes the response, which can hold customer data.
	if protojson.Unmarshal(data, &out) != nil {
		return nil, &codecError{"the codec server's response is not a list of payloads", nil}
	}
	if len(out.GetPayloads()) != len(ps) {
		return nil, &codecError{"the codec server returned a different number of payloads than it was sent", fmt.Errorf("got %d, sent %d", len(out.GetPayloads()), len(ps))}
	}
	return out.GetPayloads(), nil
}

func requestFailure(err error) string {
	var timeout interface{ Timeout() bool }
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout():
		return "timed out waiting for the codec server"
	case errors.As(err, new(*tls.CertificateVerificationError)):
		return "the TLS certificate check with the codec server failed"
	}
	return "the request to the codec server failed"
}
