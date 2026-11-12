// Package connection handles cancellation while broker clients are being opened.
package connection

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// Dialer cancels sockets during setup, then detaches them from the caller's
// context once the client has been successfully opened.
type Dialer struct {
	base     net.Dialer
	ctx      context.Context
	cancel   context.CancelFunc
	parent   context.Context
	stop     func() bool
	mu       sync.Mutex
	conns    []net.Conn
	released bool
}

func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	conn, err := d.base.DialContext(d.ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !d.released {
		d.conns = append(d.conns, conn)
	}
	return conn, nil
}

func (d *Dialer) abort() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.released {
		return
	}
	d.cancel()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.conns = nil
}

func (d *Dialer) release() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.parent.Err(); err != nil {
		return err
	}
	d.released = true
	d.conns = nil
	d.parent = context.Background()
	d.stop()
	return nil
}

// Open returns promptly on cancellation and closes any client created afterward.
// The dialer also interrupts in-flight TCP connections and protocol handshakes.
func Open[T io.Closer](ctx context.Context, closed <-chan struct{}, base net.Dialer, create func(*Dialer) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case <-closed:
		return zero, core.ErrClosed
	default:
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dialCtx, dialCancel := context.WithCancel(context.Background())
	dialer := &Dialer{base: base, ctx: dialCtx, cancel: dialCancel, parent: ctx}
	dialer.stop = context.AfterFunc(ctx, dialer.abort)
	type result struct {
		client T
		err    error
	}
	results := make(chan result)
	go func() {
		client, err := create(dialer)
		if err == nil {
			if err = dialer.release(); err != nil {
				_ = client.Close()
			}
		} else {
			dialer.abort()
		}
		select {
		case results <- result{client, err}:
		case <-ctx.Done():
			if err == nil {
				_ = client.Close()
			}
		}
	}()
	select {
	case result := <-results:
		if err := ctx.Err(); err != nil {
			if result.err == nil {
				_ = result.client.Close()
			}
			return zero, err
		}
		select {
		case <-closed:
			if result.err == nil {
				_ = result.client.Close()
			}
			return zero, core.ErrClosed
		default:
		}
		return result.client, result.err
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-closed:
		return zero, core.ErrClosed
	}
}

// Wait interrupts retry backoff when the caller cancels or the broker closes.
func Wait(ctx context.Context, closed <-chan struct{}, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closed:
		return core.ErrClosed
	case <-timer.C:
		return ctx.Err()
	}
}
