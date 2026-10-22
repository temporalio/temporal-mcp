package payload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
)

func TestRequestFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"deadline exceeded", context.DeadlineExceeded, "timed out waiting for the codec server"},
		{"wrapped deadline exceeded", fmt.Errorf("read: %w", context.DeadlineExceeded), "timed out waiting for the codec server"},
		{"transport timeout", &net.DNSError{IsTimeout: true}, "timed out waiting for the codec server"},
		{"untrusted certificate", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, "the codec server's TLS certificate does not chain to a trusted CA"},
		{"certificate for another host", &tls.CertificateVerificationError{Err: x509.HostnameError{Host: "codec.example"}}, "the codec server's TLS certificate is not valid for the codec endpoint's host"},
		{"expired certificate", &tls.CertificateVerificationError{Err: x509.CertificateInvalidError{Reason: x509.Expired}}, "the codec server's TLS certificate has expired or is not yet valid"},
		{"other certificate failure", &tls.CertificateVerificationError{Err: x509.CertificateInvalidError{Reason: x509.NotAuthorizedToSign}}, "the TLS certificate check with the codec server failed"},
		{"unknown host", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "codec.example", IsNotFound: true}}, "could not look up the codec server's host name"},
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}, "could not connect to the codec server: connection refused"},
		{"no route", &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH}}, "could not connect to the codec server: no route to host"},
		{"reset", &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}, "the codec server closed the connection without responding"},
		{"closed", io.EOF, "the codec server closed the connection without responding"},
		{"canceled", context.Canceled, "the request to the codec server failed"},
		{"other failure", errors.New(`net/http: HTTP/1.x transport connection broken: malformed HTTP status code "there"`), "the request to the codec server failed"},
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
