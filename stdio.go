package main

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type stdioTransport struct {
	mcp.IOTransport
	mcp.Connection
	eof, drained, closed chan struct{}
	mu                   sync.Mutex
	pending              int
	stopping             bool
	closeOnce            sync.Once
}

func (t *stdioTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	var err error
	t.Connection, err = t.IOTransport.Connect(ctx)
	return t, err
}

func (t *stdioTransport) Read(ctx context.Context) (jsonrpc.Message, error) {
	msg, err := t.Connection.Read(ctx)
	if errors.Is(err, io.EOF) {
		// The SDK cancels active requests on EOF. Delay it until they have
		// written their responses, keeping stdout open during the drain.
		close(t.eof)
		select {
		case <-t.closed:
		case <-ctx.Done():
		}
		return msg, err
	}
	if err != nil {
		return msg, err
	}
	t.mu.Lock()
	if t.stopping {
		t.mu.Unlock()
		select {
		case <-t.closed:
		case <-ctx.Done():
		}
		return nil, io.EOF
	}
	if req, ok := msg.(*jsonrpc.Request); ok && req.IsCall() {
		t.pending++
	}
	t.mu.Unlock()
	return msg, nil
}

func (t *stdioTransport) Write(ctx context.Context, msg jsonrpc.Message) error {
	err := t.Connection.Write(ctx, msg)
	if _, ok := msg.(*jsonrpc.Response); ok {
		t.mu.Lock()
		t.pending--
		if t.stopping && t.pending == 0 {
			close(t.drained)
		}
		t.mu.Unlock()
	}
	return err
}

// drain stops accepting requests and waits for their responses. Session.Close
// cannot start sooner: the SDK drops responses once its session starts closing.
func (t *stdioTransport) drain() {
	t.mu.Lock()
	t.stopping = true
	if t.pending == 0 {
		close(t.drained)
	}
	t.mu.Unlock()
	select {
	case <-t.drained:
	case <-t.closed:
	}
}

func (t *stdioTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return t.Connection.Close()
}
