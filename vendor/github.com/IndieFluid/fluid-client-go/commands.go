package fluidclient

import (
	"context"
	"encoding/binary"
)

func commandFrame(command string, args []byte) []byte {
	frame := make([]byte, 0, 4+len(command)+len(args))
	var sz [4]byte
	binary.BigEndian.PutUint32(sz[:], uint32(len(command)))
	frame = append(frame, sz[:]...)
	frame = append(frame, command...)
	return append(frame, args...)
}

// PushTask enqueues payload as a task via the "push" command.
// After a worker handles it, the response comes back through
// Receive/Serve on this same connection.
func (c *Client) PushTask(ctx context.Context, payload []byte) error {
	return c.SendMessage(ctx, commandFrame("push", payload))
}

// AddWorkers asks the proxy to start (or grow) its worker pool.
func (c *Client) AddWorkers(ctx context.Context, n uint32) error {
	var args [4]byte
	binary.BigEndian.PutUint32(args[:], n)
	return c.SendMessage(ctx, commandFrame("add", args[:]))
}

// RemoveWorkers asks the proxy to shrink its worker pool.
func (c *Client) RemoveWorkers(ctx context.Context, n uint32) error {
	var args [4]byte
	binary.BigEndian.PutUint32(args[:], n)
	return c.SendMessage(ctx, commandFrame("remove", args[:]))
}
