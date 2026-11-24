package fixture

import (
	"context"
	"time"

	"go.temporal.io/sdk/testsuite"
)

// StartDevServer is testsuite.StartDevServer, retried so that a transient failure, such as a connection reset while
// downloading the Temporal CLI, does not fail the test.
func StartDevServer(ctx context.Context, options testsuite.DevServerOptions) (*testsuite.DevServer, error) {
	for attempt := 1; ; attempt++ {
		dev, err := testsuite.StartDevServer(ctx, options)
		if err == nil || attempt == 3 {
			return dev, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(5 * time.Second):
		}
	}
}
