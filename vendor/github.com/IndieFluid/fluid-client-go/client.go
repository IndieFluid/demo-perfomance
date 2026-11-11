// Package fluidclient is a TCP client for the fluid-core proxy.
//
// Wire format is identical in both directions:
//
//	[4-byte big-endian length][payload]
//
// Command payloads are [4-byte commandSize][command][args]
// ("add", "remove", "push"); anything else is treated as a task
// message and pushed to the proxy's queue.
package fluidclient

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Client is a TCP connection to a fluid-core proxy. Send methods are
// safe for concurrent use; receive methods are not — run a single
// Receive/Serve loop per connection.
type Client struct {
	conn net.Conn
	mu   sync.Mutex
}

func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// SendMessage writes one raw payload frame.
func (c *Client) SendMessage(ctx context.Context, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetWriteDeadline(deadline)
		defer c.conn.SetWriteDeadline(time.Time{})
	}
	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(payload)))
	if _, err := c.conn.Write(append(sz[:], payload...)); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// Receive reads exactly one inbound frame.
func (c *Client) Receive(ctx context.Context) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetReadDeadline(deadline)
		defer c.conn.SetReadDeadline(time.Time{})
	}
	var sz [4]byte
	if _, err := io.ReadFull(c.conn, sz[:]); err != nil {
		return nil, fmt.Errorf("read frame size: %w", err)
	}
	buf := make([]byte, binary.BigEndian.Uint32(sz[:]))
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}
	return buf, nil
}

// Serve reads frames and invokes handler for each until ctx is
// cancelled or the connection fails. It is the receive loop for
// tasks dispatched back by the proxy's workers.
func (c *Client) Serve(ctx context.Context, handler func(msg []byte)) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.conn.Close()
		case <-done:
		}
	}()

	for {
		msg, err := c.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		handler(msg)
	}
}
