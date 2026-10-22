// The codec server client, which speaks the protocol of the Temporal CLI's
// --codec-endpoint: a POST to the endpoint's /decode path, with the payloads as
// temporal.api.common.v1.Payloads in proto JSON, the target's namespace in the
// X-Namespace header, and the configured Authorization header.

package payload

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// codecTimeout bounds a codec request: a tool call's own context ends only when its client goes away.
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

// A codecError is a failed codec request. Its reason, which check_connection and each
// payload the request leaves out show agents, says what went wrong in plain words
// and names no address, path or response content. Its cause, for the log, may name
// an address or path but never response content; it is nil when the reason says it all.
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

// decode returns ps as the codec server decodes them, in order.
func (c *codec) decode(ctx context.Context, ps []*commonpb.Payload) ([]*commonpb.Payload, *codecError) {
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

// requestFailure words a failed codec request by its kind alone: err names the
// endpoint's URL, and a certificate or DNS error may name hosts.
func requestFailure(err error) string {
	var timeout interface{ Timeout() bool }
	var invalid x509.CertificateInvalidError
	var errno syscall.Errno
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout():
		return "timed out waiting for the codec server"
	case errors.As(err, new(x509.UnknownAuthorityError)):
		return "the codec server's TLS certificate does not chain to a trusted CA"
	case errors.As(err, new(x509.HostnameError)):
		return "the codec server's TLS certificate is not valid for the codec endpoint's host"
	case errors.As(err, &invalid) && invalid.Reason == x509.Expired:
		return "the codec server's TLS certificate has expired or is not yet valid"
	case errors.As(err, new(*tls.CertificateVerificationError)):
		return "the TLS certificate check with the codec server failed"
	case errors.As(err, new(*net.DNSError)):
		return "could not look up the codec server's host name"
	case errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE):
		return "the codec server closed the connection without responding"
	case errors.As(err, &errno):
		// An errno's text, such as connection refused, names no address.
		return "could not connect to the codec server: " + errno.Error()
	}
	return "the request to the codec server failed"
}
