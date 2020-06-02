package pulsar

import (
	"context"
	"net"
)

type dialer struct {
	log  Logger
	host string
}

func newDialer(log Logger, host string) *dialer {
	return &dialer{
		log:  log,
		host: host,
	}
}

func (d *dialer) connect(ctx context.Context) (*conn, error) {
	dial := net.Dialer{}
	netConn, err := dial.DialContext(ctx, "tcp", d.host)
	if err != nil {
		return nil, err
	}

	conn := newConn(d.log, netConn)
	return conn, nil
}
