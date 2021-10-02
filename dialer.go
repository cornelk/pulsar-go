package pulsar

import (
	"context"
	"net"
)

type dialer func(ctx context.Context, log Logger, host string) (*conn, error)

func defaultDialer(ctx context.Context, log Logger, host string) (*conn, error) {
	dial := net.Dialer{}
	netConn, err := dial.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}

	c := newConn(log, netConn)
	return c, nil
}
