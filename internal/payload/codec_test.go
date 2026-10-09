package payload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"testing"
)

func TestRequestFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"deadline exceeded", context.DeadlineExceeded, "timed out waiting for the codec server"},
		{"transport timeout", &net.DNSError{IsTimeout: true}, "timed out waiting for the codec server"},
		{"untrusted certificate", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, "the TLS certificate check with the codec server failed"},
		{"canceled", context.Canceled, "the request to the codec server failed"},
		{"other failure", errors.New("connection refused"), "the request to the codec server failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// net/http wraps transport failures in a URL error.
			err := &url.Error{Op: "Post", URL: "https://codec.example/decode", Err: tt.err}
			if got := requestFailure(err); got != tt.want {
				t.Errorf("requestFailure = %q, want %q", got, tt.want)
			}
		})
	}
}
